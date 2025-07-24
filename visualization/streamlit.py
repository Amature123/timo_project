# Combine all issues
all_issues = []

for entry in violations_1["violations"]:
    all_issues.extend(entry["issues"])

for entry in violations_2["violations"]:
    all_issues.extend(entry["issues"])

# Count frequency
issue_counts = Counter(all_issues)
top_issues_df = pd.DataFrame(issue_counts.items(), columns=["Issue", "Count"]).sort_values("Count", ascending=False)

# Risky transaction = high-value or limit exceeded
risky_keywords = ["High-value transaction", "Daily total"]
risky_issues = [issue for issue in all_issues if any(key in issue for key in risky_keywords)]

# Unverified devices: from violations_1
unverified_devices = [entry["customer_id"] for entry in violations_1["violations"]]

# -------------------- Streamlit Layout --------------------

st.set_page_config(page_title="Timo Risk Dashboard", layout="wide")

st.title("📊 Timo Data Quality & Risk Dashboard")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🚨 Top Violations", len(top_issues_df))
    st.dataframe(top_issues_df, use_container_width=True)

with col2:
    st.metric("💸 Risky Transactions", len(risky_issues))

with col3:
    st.metric("📱 Unverified Devices", len(unverified_devices))
    st.dataframe(pd.DataFrame(unverified_devices, columns=["Customer ID"]), use_container_width=True)