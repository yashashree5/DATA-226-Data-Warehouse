# Homework 7 — dbt + Snowflake

This project demonstrates building dbt models on Snowflake.  
It creates input views from **RAW** tables, transforms them into the **ANALYTICS** schema,  
builds the **`SESSION_SUMMARY`** model, snapshots it for historical tracking,  
and validates data integrity using dbt tests.

---

## 📁 Project Overview

### Models
| Layer | Description |
|--------|--------------|
| **input/** | Reads data from RAW tables (`user_session_channel`, `session_timestamp`) |
| **output/** | Builds `session_summary` model joining input views |
| **snapshots/** | Contains `snapshot_session_summary.sql` for historical tracking |

---

## 🧱 Key Objects

| Object | Type | Description |
|---------|------|-------------|
| `RAW.USER_SESSION_CHANNEL` | Source table | Raw user-session data |
| `RAW.SESSION_TIMESTAMP` | Source table | Raw timestamp data |
| `ANALYTICS.SESSION_SUMMARY` | Model | Joined analytics view with `userId`, `sessionId`, `channel`, `ts` |
| `SNAPSHOT.SNAPSHOT_SESSION_SUMMARY` | Snapshot | Historical versioned table of `session_summary` |

---

## 🧪 Tests

Applied on `session_summary.sessionId`:
- `not_null`
- `unique`

---

## 🚀 How to Run

Run the following commands from your dbt project root:

```bash
# Verify dbt + Snowflake connection
dbt debug

# Build input layer models
dbt run -s "path:models/input/*"

# Build output (analytics) models
dbt run -s output.session_summary

# Run snapshot to track history
dbt snapshot

# Run data tests on session_summary
dbt test -s session_summary
