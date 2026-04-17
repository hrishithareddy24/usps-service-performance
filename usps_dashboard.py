import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import storage
from google.oauth2.credentials import Credentials
import io

st.set_page_config(page_title="USPS Service Performance Dashboard", layout="wide")
st.title("USPS Service Performance Insights Across Rural America")
st.markdown("**Team Nexus Ninjas | Challenge X | George Mason University**")
st.markdown("---")

@st.cache_data(ttl=3600)
def load_data():
    creds = Credentials(
        token=None,
        refresh_token=st.secrets["gcp_credentials"]["refresh_token"],
        client_id=st.secrets["gcp_credentials"]["client_id"],
        client_secret=st.secrets["gcp_credentials"]["client_secret"],
        token_uri="https://oauth2.googleapis.com/token"
    )
    client = storage.Client(project='project-4be3a115-f3f9-404e-8ba', credentials=creds)
    bucket = client.bucket('usps-pipeline-data')

    def read_folder(folder):
        blobs = list(bucket.list_blobs(prefix=folder))
        dfs = []
        for blob in blobs:
            if blob.name.endswith('.csv'):
                content = blob.download_as_bytes()
                dfs.append(pd.read_csv(io.BytesIO(content)))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    rural_urban = read_folder('results/rural_urban_summary/')
    district = read_folder('results/district_summary/')
    mailtype = read_folder('results/mailtype_summary/')
    origin = read_folder('results/origin_summary/')
    dest = read_folder('results/dest_summary/')

    rural_urban = rural_urban.dropna(subset=['rural_urban']).copy()
    rural_urban['rural_urban'] = rural_urban['rural_urban'].map({'Yes': 'Rural', 'No': 'Urban'})
    rural_urban['avg_score_pct'] = rural_urban['avg_score'] * 100

    district = district.dropna(subset=['avg_score']).copy()
    district['avg_score_pct'] = district['avg_score'] * 100

    mailtype = mailtype.dropna(subset=['rural_urban']).copy()
    mailtype['avg_score_pct'] = mailtype['avg_score'] * 100
    mailtype['rural_urban_label'] = mailtype['rural_urban'].map({'Yes': 'Rural', 'No': 'Urban'})
    mailtype['mail_simple'] = mailtype['prodt'].str[:45]

    origin['avg_score_pct'] = origin['avg_score'] * 100
    origin['type'] = 'Sending (Origin)'

    dest['avg_score_pct'] = dest['avg_score'] * 100
    dest['type'] = 'Receiving (Destination)'
    dest = dest.rename(columns={'dest_rural_label': 'origin_rural_label'})

    return rural_urban, district, mailtype, origin, dest

with st.spinner("Loading latest data from GCP..."):
    rural_urban, district, mailtype, origin, dest = load_data()

if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.caption(f"Data loaded: {rural_urban['total_records'].sum():,.0f} total records analyzed")
st.markdown("---")

st.subheader("Key Performance Metrics")
rural = rural_urban[rural_urban['rural_urban'] == 'Rural'].iloc[0]
urban = rural_urban[rural_urban['rural_urban'] == 'Urban'].iloc[0]
fy26_target = 89.0

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Rural On-Time Rate", f"{rural['avg_score_pct']:.1f}%", f"{rural['avg_score_pct'] - fy26_target:.1f}% vs FY26 Target", delta_color="inverse")
col2.metric("Urban On-Time Rate", f"{urban['avg_score_pct']:.1f}%", f"{urban['avg_score_pct'] - fy26_target:.1f}% vs FY26 Target", delta_color="inverse")
col3.metric("Rural vs Urban Gap", f"{abs(rural['avg_score_pct'] - urban['avg_score_pct']):.1f}%", "Rural higher" if rural['avg_score_pct'] > urban['avg_score_pct'] else "Urban higher")
col4.metric("Rural Avg Days", f"{rural['avg_days']:.2f}", f"{rural['avg_days'] - urban['avg_days']:.2f} vs Urban")
col5.metric("Total Records", f"{rural_urban['total_records'].sum()/1e6:.0f}M", "rows analyzed")

st.markdown("---")

st.subheader("Sending vs Receiving Performance")
combined = pd.concat([origin, dest], ignore_index=True)
fig0 = px.bar(combined, x='origin_rural_label', y='avg_score_pct',
              color='type', barmode='group',
              color_discrete_map={'Sending (Origin)': '#1D9E75', 'Receiving (Destination)': '#185FA5'},
              text='avg_score_pct',
              labels={'avg_score_pct': 'On-Time Rate (%)', 'origin_rural_label': '', 'type': ''})
fig0.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig0.add_hline(y=fy26_target, line_dash="dash", line_color="red", annotation_text=f"FY26 Target ({fy26_target}%)")
fig0.update_layout(yaxis_range=[78, 92])
st.plotly_chart(fig0, use_container_width=True)

st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    st.subheader("On-Time Rate: Rural vs Urban")
    fig1 = px.bar(rural_urban, x='rural_urban', y='avg_score_pct',
                  color='rural_urban',
                  color_discrete_map={'Rural': '#E8593C', 'Urban': '#1D9E75'},
                  text='avg_score_pct',
                  labels={'avg_score_pct': 'On-Time Rate (%)', 'rural_urban': ''})
    fig1.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig1.add_hline(y=fy26_target, line_dash="dash", line_color="red", annotation_text=f"FY26 Target ({fy26_target}%)")
    fig1.update_layout(showlegend=False, yaxis_range=[80, 83])
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Average Days to Deliver")
    fig2 = px.bar(rural_urban, x='rural_urban', y='avg_days',
                  color='rural_urban',
                  color_discrete_map={'Rural': '#E8593C', 'Urban': '#1D9E75'},
                  text='avg_days',
                  labels={'avg_days': 'Days', 'rural_urban': ''})
    fig2.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig2.update_layout(showlegend=False, yaxis_range=[rural_urban['avg_days'].min()-0.3, rural_urban['avg_days'].max()+0.3])
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

st.subheader("Worst Performing Rural Districts")
district_rural = district[district['rural_urban'] == 'Yes'].sort_values('avg_score')
rural_avg = rural_urban[rural_urban['rural_urban'] == 'Rural']['avg_score_pct'].iloc[0]
num_districts = st.slider("Number of districts to show", 5, 20, 10)
district_filtered = district_rural.head(num_districts)

fig3 = px.bar(district_filtered, x='avg_score_pct', y='orgn_dist_name',
              orientation='h',
              color='avg_score_pct',
              color_continuous_scale=['#E8593C', '#F9CB42', '#1D9E75'],
              text='avg_score_pct',
              labels={'avg_score_pct': 'On-Time Rate (%)', 'orgn_dist_name': 'District'})
fig3.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig3.add_vline(x=fy26_target, line_dash="dash", line_color="red", annotation_text=f"FY26 Target ({fy26_target}%)")
fig3.add_vline(x=rural_avg, line_dash="dash", line_color="orange", annotation_text=f"Rural Avg ({rural_avg:.1f}%)")
fig3.update_layout(coloraxis_showscale=False, xaxis_range=[district_filtered['avg_score_pct'].min()-5, 95])
st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

st.subheader("Worst Performing Rural Mail Types")
rural_mail = mailtype[mailtype['rural_urban'] == 'Yes'].copy()
rural_mail = rural_mail[rural_mail['avg_score_pct'] <= 100]
rural_mail['mail_short'] = rural_mail['prodt'].str[:50]
rural_mail = rural_mail.sort_values('avg_score_pct').drop_duplicates(subset=['mail_short']).head(10)

fig5 = px.bar(rural_mail, x='avg_score_pct', y='mail_short',
              orientation='h',
              color='avg_score_pct',
              color_continuous_scale=['#E8593C', '#F9CB42', '#1D9E75'],
              text='avg_score_pct',
              labels={'avg_score_pct': 'On-Time Rate (%)', 'mail_short': 'Mail Type'})
fig5.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig5.add_vline(x=89, line_dash="dash", line_color="red", annotation_text="FY26 Target")
fig5.update_layout(coloraxis_showscale=False, xaxis_range=[0, 100])
st.plotly_chart(fig5, use_container_width=True)

st.markdown("---")
st.caption("Data source: USPS Service Performance Dashboard (spm.usps.com) | Rural classification: HRSA FORHP | Pipeline: Apache Spark on GCP Dataproc | Team Nexus Ninjas | Challenge X | GMU")
