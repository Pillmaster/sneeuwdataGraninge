import streamlit as st
import requests
import pandas as pd
import datetime

# --- APP CONFIGURATIE ---
# ===============================================================================
st.set_page_config(
    page_title="Historische Sneeuwdiepte Zoeker",
    layout="wide"
)

# --- INSTELWAARDEN ---
# Coördinaten van Malmån (gebruikt in de originele scripts)
LAT = 62.9977
LON = 17.0811
TIMEZONE = 'Europe/Stockholm'
MAX_HISTORICAL_YEAR = datetime.date.today().year # Dit is nu 2025

# Maximale datum is gisteren, omdat historische data niet recenter kan zijn
MAX_DATE_SELECTABLE = datetime.date.today() - datetime.timedelta(days=1)
# Minimaal jaar ingesteld op 1950 (voor betrouwbare records)
MIN_DATE_SELECTABLE = datetime.date(1950, 1, 1) 

# --- DATA FUNCTIE VOOR EEN ENKELE PERIODE (GEEN CACHING) ---
# ===============================================================================

def fetch_single_period_data(lat, lon, start, end):
    """Haalt de uurlijkse sneeuwdiepte op en aggregeert deze naar dagelijkse max in cm voor EEN KORTE PERIODE."""
    
    api_url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": "snow_depth", 
        "timezone": TIMEZONE
    }
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        st.error(f"❌ Fout bij het ophalen van data van de Open-Meteo API voor {start} t/m {end}: {e}")
        return None
        
    if "hourly" not in data or "snow_depth" not in data["hourly"]:
        return None

    # --- Data in DataFrame ---
    df = pd.DataFrame({
        "time": data["hourly"]["time"],
        "snow_depth_m": data["hourly"]["snow_depth"]
    })

    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")

    daily_max = df["snow_depth_m"].resample('D').max()
    daily_cm = (daily_max * 100).round(1).to_frame(name="Max Sneeuwdiepte (cm)")
    daily_cm['Max Sneeuwdiepte (cm)'] = daily_cm['Max Sneeuwdiepte (cm)'].fillna(0.0)
    
    return daily_cm


# --- HOOFDFUNCTIE VOOR HET OPHALEN VAN ALLE DATA (MET CACHING EN CHUNKING) ---
# ===============================================================================

@st.cache_data(ttl=86400, show_spinner="Laden historische sneeuwdiepte data in 4-jarige delen...")
def get_all_historical_data(lat, lon, start_date_api, end_date_api, chunk_size_years=4):
    """Haalt alle data op door de periode op te splitsen in chunks van 4 jaar."""
    
    try:
        start_date = datetime.datetime.strptime(start_date_api, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date_api, "%Y-%m-%d").date()
    except ValueError:
        st.error("Ongeldige datumstring voor de API-aanroep.")
        return None

    all_data_frames = []
    current_start = start_date

    while current_start <= end_date:
        # Bereken het einde van de huidige 4-jarige chunk
        chunk_end_candidate = current_start + datetime.timedelta(days=chunk_size_years * 365 + 10) 
        
        # Bepaal het werkelijke einde van de chunk
        chunk_end = min(chunk_end_candidate, end_date)

        st.toast(f"Laden data van: {current_start.strftime('%d-%m-%Y')} t/m {chunk_end.strftime('%d-%m-%Y')}")
        
        chunk_df = fetch_single_period_data(lat, lon, current_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))

        if chunk_df is not None:
            all_data_frames.append(chunk_df)

        if chunk_end == end_date:
            break

        current_start = chunk_end + datetime.timedelta(days=1)
        
        if current_start > end_date:
            break

    if not all_data_frames:
        st.error("Geen data kunnen ophalen. Controleer de API-connectie of de gekozen periode.")
        return None

    combined_df = pd.concat(all_data_frames)
    combined_df = combined_df[~combined_df.index.duplicated(keep='first')]

    return combined_df

# --- HELPER FUNCTIE VOOR SEIZOENS-DAGNUMMER ---

def get_seasonal_ordinal(date):
    """
    Berekent een ordinaal dagnummer binnen het winterseizoen, startend op 1 juli (dag 1).
    Dit nummer staat los van het jaar en is cruciaal voor correcte Vroegste/Laatste sortering.
    """
    
    # Bepaal de dag van 30 juni in het jaar van de datum (kan 181 of 182 zijn i.v.m. schrikkeljaar)
    try:
        jun_30_ordinal = datetime.date(date.year, 6, 30).timetuple().tm_yday
    except ValueError:
        jun_30_ordinal = date.replace(month=6, day=30).timetuple().tm_yday

    if date.month >= 7:
        # Datum in de eerste seizoenshelft (Jul 1 t/m Dec 31)
        return date.timetuple().tm_yday - jun_30_ordinal
    else:
        # Datum in de tweede seizoenshelft (Jan 1 t/m Jun 30)
        prev_year = date.year - 1
        
        # Aantal dagen in het tweede halfjaar van vorig jaar
        prev_year_dec_31_ordinal = datetime.date(prev_year, 12, 31).timetuple().tm_yday
        prev_year_jun_30_ordinal = datetime.date(prev_year, 6, 30).timetuple().tm_yday
        days_in_prev_second_half = prev_year_dec_31_ordinal - prev_year_jun_30_ordinal
        
        return date.timetuple().tm_yday + days_in_prev_second_half

# --- RECORD CALCULATIE FUNCTIE (NU ZONDER PANDAS WARNINGS) ---
# ===============================================================================

def calculate_snow_records(df):
    """
    Berekent de gevraagde sneeuwrecords.
    Het seizoen loopt van 01-07 jaar X t/m 30-06 jaar X+1 (Winterseizoen X/X+1).
    """
    
    temp_df = df.copy()
    
    # --- Voorbereiding: Toevoegen Seizoen-label ---
    year_start = temp_df.index.year.where(temp_df.index.month >= 7, temp_df.index.year - 1)
    year_end = year_start + 1
    
    temp_df['Seizoen'] = year_start.astype(str) + '/' + year_end.astype(str)
    
    snow_days = temp_df[temp_df['Max Sneeuwdiepte (cm)'] > 0.0]
    
    if snow_days.empty:
        return None 

    results = {}
    
    # 1. Top 10 Seizoenen met Hoogste Maximum Sneeuwhoogte
    yearly_max = temp_df.groupby('Seizoen')['Max Sneeuwdiepte (cm)'].max().sort_values(ascending=False).head(10)
    results['Top 10 Hoogste Max'] = yearly_max.reset_index().rename(columns={'Max Sneeuwdiepte (cm)': 'Max (cm)'})
    
    
    # --- Voorbereiding: Vroegste en Laatste Sneeuw Dag per Seizoen ---
    # Gebruik include_groups=False om Pandas FutureWarning te verhelpen
    earliest_snow_all = snow_days.groupby('Seizoen').apply(lambda x: x.index.min(), include_groups=False).to_frame(name='Datum')
    latest_snow_all = snow_days.groupby('Seizoen').apply(lambda x: x.index.max(), include_groups=False).to_frame(name='Datum')
    
    
    # 2. Top 10 Vroegste Start van Sneeuwdek (Min. Seasonal Ordinal van de eerste sneeuwdag)
    earliest_start_df = earliest_snow_all.copy()
    earliest_start_df['Seasonal_Ordinal'] = earliest_start_df['Datum'].apply(get_seasonal_ordinal)
    earliest_start_df = earliest_start_df.sort_values(by='Seasonal_Ordinal', ascending=True).head(10).reset_index(drop=False) 
    earliest_start_df['Vroegste Start (DD-MM)'] = earliest_start_df['Datum'].dt.strftime("%d-%m")
    results['Top 10 Vroegste Start Sneeuwdek'] = earliest_start_df[['Vroegste Start (DD-MM)', 'Seizoen']]
    
    
    # 3. Top 10 Laatste Start van Sneeuwdek (Max. Seasonal Ordinal van de eerste sneeuwdag)
    latest_start_df = earliest_snow_all.copy()
    latest_start_df['Seasonal_Ordinal'] = latest_start_df['Datum'].apply(get_seasonal_ordinal)
    latest_start_df = latest_start_df.sort_values(by='Seasonal_Ordinal', ascending=False).head(10).reset_index(drop=False) 
    latest_start_df['Laatste Start (DD-MM)'] = latest_start_df['Datum'].dt.strftime("%d-%m")
    results['Top 10 Laatste Start Sneeuwdek'] = latest_start_df[['Laatste Start (DD-MM)', 'Seizoen']]
    

    # 4. Top 10 Vroegste Einde van Sneeuwdek (Min. Seasonal Ordinal van de laatste sneeuwdag)
    earliest_end_df = latest_snow_all.copy()
    earliest_end_df['Seasonal_Ordinal'] = earliest_end_df['Datum'].apply(get_seasonal_ordinal)
    earliest_end_df = earliest_end_df.sort_values(by='Seasonal_Ordinal', ascending=True).head(10).reset_index(drop=False)
    earliest_end_df['Vroegste Einde (DD-MM)'] = earliest_end_df['Datum'].dt.strftime("%d-%m")
    results['Top 10 Vroegste Einde Sneeuwdek'] = earliest_end_df[['Vroegste Einde (DD-MM)', 'Seizoen']]


    # 5. Top 10 Laatste Einde van Sneeuwdek (Max. Seasonal Ordinal van de laatste sneeuwdag)
    latest_end_df = latest_snow_all.copy()
    latest_end_df['Seasonal_Ordinal'] = latest_end_df['Datum'].apply(get_seasonal_ordinal)
    latest_end_df = latest_end_df.sort_values(by='Seasonal_Ordinal', ascending=False).head(10).reset_index(drop=False)
    latest_end_df['Laatste Einde (DD-MM)'] = latest_end_df['Datum'].dt.strftime("%d-%m")
    results['Top 10 Laatste Einde Sneeuwdek'] = latest_end_df[['Laatste Einde (DD-MM)', 'Seizoen']]


    # 6. Top 10 Meeste Dagen met Sneeuwdek per Seizoen
    snow_days_per_season = snow_days.groupby('Seizoen').size().sort_values(ascending=False).head(10)
    results['Top 10 Meeste Dagen'] = snow_days_per_season.reset_index().rename(columns={0: 'Aantal Dagen'})
    
    
    # 7. Langste onafgebroken periode met sneeuwdek (ABSOLUUT RECORD over de HELE ANALYSEPERIODE)
    snow_present = (temp_df['Max Sneeuwdiepte (cm)'] > 0.0)
    snow_present_days = snow_present[snow_present].index.to_series()
    
    if not snow_present_days.empty:
        consecutive_groups = snow_present_days.diff().dt.days.ne(1).cumsum()
        
        streak_lengths = consecutive_groups.value_counts()
        
        longest_streak_length = streak_lengths.max()
        longest_streak_group = streak_lengths.idxmax()
        
        # Voor de metrische weergave is DD-MM-YYYY prima leesbaar
        longest_streak_start = snow_present_days[consecutive_groups == longest_streak_group].min().strftime("%d-%m-%Y")
        longest_streak_end = snow_present_days[consecutive_groups == longest_streak_group].max().strftime("%d-%m-%Y")

        results['Absoluut Langste Periode'] = f"{longest_streak_length} dagen ({longest_streak_start} t/m {longest_streak_end})"
        
        # 8. Totaal aantal dagen met sneeuw (over de hele periode)
        results['Totaal Dagen met Sneeuw'] = snow_days.shape[0]

    return results


# --- STREAMLIT WEERGAVE & INPUT ---
# ===============================================================================
st.title("🏔️ Analyse van Dagelijkse Max Sneeuwdiepte")
st.markdown(f"Gegevens voor coördinaten: **{LAT}°N, {LON}°E** via Open-Meteo Archive API.")
st.markdown("---")

# --- Deel 1: Input voor Records Analyse ---
st.sidebar.header("Record Analyse Periode")
st.sidebar.info("Data wordt automatisch in kleine delen opgehaald om API-limieten te omzeilen.")

# Kies het jaarbereik voor de records. 
start_year = st.sidebar.number_input(
    "Startjaar (Min. 1950):", 
    min_value=1950,
    max_value=MAX_HISTORICAL_YEAR, 
    value=2015,
    step=1, 
    key="record_start_year"
)

# Aangepaste input: max_value is nu MAX_HISTORICAL_YEAR (huidig jaar) om het laatste complete seizoen te pakken
end_year = st.sidebar.number_input(
    "Eindjaar seizoen (Max. Huidig jaar, einde op 30 juni):", 
    min_value=start_year, 
    max_value=MAX_HISTORICAL_YEAR, # MAXIMALE WAARDE IS NU HET HUIDIGE JAAR (2025)
    value=MAX_HISTORICAL_YEAR, # DEFAULT WAARDE IS NU HET HUIDIGE JAAR (2025)
    step=1, 
    key="record_end_year"
)

# Bereken de start- en einddatum voor de API-call op basis van het winterseizoen (01-07 t/m 30-06)
record_start_date_api = datetime.date(start_year, 7, 1).strftime("%Y-%m-%d")
record_end_date_api = datetime.date(end_year, 6, 30).strftime("%Y-%m-%d")

# Haal de data op voor de volledige recordanalyse met chunking
full_data = get_all_historical_data(LAT, LON, record_start_date_api, record_end_date_api)

if full_data is not None:
    records = calculate_snow_records(full_data)
    
    st.header("🏆 Historische Sneeuwrecords per Seizoen")
    st.markdown(f"Analyse over seizoenen **{start_year}/{start_year+1}** t/m **{end_year-1}/{end_year}**.")
    
    if records:
        
        col1, col2 = st.columns(2)
        
        # Kolom 1: Max Diepte en Aantal Dagen
        with col1:
            st.subheader("Hoogste Max Sneeuwdiepte")
            st.dataframe(records['Top 10 Hoogste Max'], width='stretch', hide_index=True)

        with col2:
            st.subheader("Meeste Dagen met Sneeuwdek")
            st.dataframe(records['Top 10 Meeste Dagen'], width='stretch', hide_index=True)
            
        st.markdown("---")
        
        # Kolom 2: Vroegste en Laatste Sneeuw (Nu opgedeeld in vier kolommen)
        st.subheader("Records voor Start en Einde van het Seizoen")
        col3, col4, col5, col6 = st.columns(4)

        with col3:
            st.markdown("**Vroegste Start**")
            st.dataframe(records['Top 10 Vroegste Start Sneeuwdek'], width='stretch', hide_index=True)

        with col4:
            st.markdown("**Laatste Start**")
            st.dataframe(records['Top 10 Laatste Start Sneeuwdek'], width='stretch', hide_index=True)
            
        with col5:
            st.markdown("**Vroegste Einde**")
            st.dataframe(records['Top 10 Vroegste Einde Sneeuwdek'], width='stretch', hide_index=True)

        with col6:
            st.markdown("**Laatste Einde**")
            st.dataframe(records['Top 10 Laatste Einde Sneeuwdek'], width='stretch', hide_index=True)

        st.markdown("---")
        
        st.subheader("Absoluut Record")
        st.metric(
            label="Langste Ononderbroken Periode (over alle jaren)",
            value=records['Absoluut Langste Periode']
        )
            
    else:
        st.warning(f"Geen sneeuwdek gevonden in de seizoenen van {start_year} t/m {end_year}.")
        
st.markdown("---")
st.markdown("---")


# --- Deel 2: Input voor Detail Analyse ---
st.sidebar.header("Detail Periode Selectie")

# Default periode: laatste maand van het vorige jaar
# Let op: De detailanalyse gebruikt de geselecteerde MAX_HISTORICAL_YEAR - 1 (2024) als default voor het detailvenster
default_start = datetime.date(MAX_HISTORICAL_YEAR - 1, 12, 1)
default_end = datetime.date(MAX_HISTORICAL_YEAR - 1, 12, 31)

start_date = st.sidebar.date_input(
    "1. Kies de Startdatum:",
    value=default_start,
    min_value=MIN_DATE_SELECTABLE, 
    max_value=MAX_DATE_SELECTABLE,
    key="snow_depth_start_date"
)

# Zorg ervoor dat de einddatum minimaal de startdatum is
min_end_date = start_date 
if min_end_date > MAX_DATE_SELECTABLE:
    min_end_date = MAX_DATE_SELECTABLE

end_date = st.sidebar.date_input(
    "2. Kies de Einddatum:",
    value=default_end if default_end >= start_date else start_date,
    min_value=min_end_date, 
    max_value=MAX_DATE_SELECTABLE, 
    key="snow_depth_end_date"
)

# Validatie en weergave
if start_date and end_date:
    if start_date > end_date:
        st.error("❌ De Startdatum kan niet na de Einddatum liggen.")
        st.stop()
    
    start_date_str_api = start_date.strftime("%Y-%m-%d")
    end_date_str_api = end_date.strftime("%Y-%m-%d")

    start_date_display = start_date.strftime("%d-%m-%Y")
    end_date_display = end_date.strftime("%d-%m-%Y")

    st.subheader("🔍 Detail Analyse")
    st.info(f"Analyseperiode: **{start_date_display}** t/m **{end_date_display}**")
else:
    st.warning("Kies alstublieft zowel een start- als een einddatum voor de detail analyse om door te gaan.")
    st.stop()


# --- UITVOERING EN WEERGAVE IN DE APP (DETAIL) ---
# ===============================================================================

# Gebruik de functie voor korte periodes voor de detail analyse
daily_data_detail = fetch_single_period_data(LAT, LON, start_date_str_api, end_date_str_api)

if daily_data_detail is not None:
    
    st.subheader("📊 Dagelijkse Max Sneeuwdiepte")
    
    # 1. Grafiek
    st.line_chart(
        daily_data_detail, 
        width='stretch'
    )

    # 2. Dataframe
    st.subheader("Data Overzicht")
    daily_data_display = daily_data_detail.copy()
    daily_data_display.index = daily_data_detail.index.strftime("%d-%m-%Y")
    st.dataframe(daily_data_display, width='stretch')

    # 3. Samenvatting
    st.markdown("---")
    st.subheader("Statistieken")
    
    col_max, col_avg, col_days = st.columns(3)
    
    max_depth = daily_data_detail['Max Sneeuwdiepte (cm)'].max()
    avg_depth = daily_data_detail['Max Sneeuwdiepte (cm)'].mean()
    days_with_snow = len(daily_data_detail[daily_data_detail['Max Sneeuwdiepte (cm)'] > 0.0])
    
    with col_max:
        st.metric(label="Hoogste Sneeuwdiepte", value=f"{max_depth:.1f} cm")
        
    with col_avg:
        st.metric(label="Gemiddelde Sneeuwdiepte (hele periode)", value=f"{avg_depth:.1f} cm")
        
    with col_days:
        st.metric(label="Aantal dagen met Sneeuw (> 0 cm)", value=f"{days_with_snow} dagen")
