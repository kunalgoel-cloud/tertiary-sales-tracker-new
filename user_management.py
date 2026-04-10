"""
user_management.py — Mamanourish User Management System
═══════════════════════════════════════════════════════════

HOW IT WORKS:
─────────────
1. Users are stored in Supabase table `app_users`:
       id, username, password_hash, salt, allowed_tabs (JSON), created_at

2. Passwords are hashed with SHA-256 + per-user random salt.
   Plaintext passwords are NEVER stored.

3. Access control:
   - On login, the user's allowed_tabs list loads into st.session_state["allowed_tabs"]
   - app.py reads this list to gate each tab with is_tab_allowed(tab_key)
   - Restricted tabs show a styled "Access Restricted" placeholder

4. Admin role (from Streamlit secrets) always has full access.

5. Tab keys (canonical slugs used in allowed_tabs JSON):
     analytics, deep_dive, marketing, upload, monthly_upload,
     configuration, channel_performance, vending, sop

DB SCHEMA — run once in Supabase SQL Editor:
──────────────────────────────────────────────
  CREATE TABLE IF NOT EXISTS app_users (
    id            SERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    salt          TEXT NOT NULL,
    allowed_tabs  JSONB DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ DEFAULT now()
  );
"""

import hashlib
import json
import re
import secrets
import streamlit as st
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

MAX_USERS = 10

# Ordered list of (tab_key, display_label) for ALL tabs in the app
# Canonical tab registry — (key, display_label).
# Keys MUST match _tidx() calls in app.py exactly.
ALL_TABS = [
    ("trend_analytics",     "📊 Trend Analytics"),
    ("deep_dive",           "🔬 Deep Dive"),
    ("performance_marketing","📣 Performance Marketing"),
    ("smart_upload",        "📤 Smart Upload"),
    ("monthly_upload",      "📅 Monthly Channel Upload"),
    ("configuration",       "🛠 Configuration"),
    ("channel_performance", "📦 Channel Performance"),
    ("vending",             "🎰 Vending"),
    ("sop",                 "📋 S&OP"),
]

# These tabs are only ever shown to the built-in admin — not grantable to custom users
# Keys must match ALL_TABS above exactly.
ADMIN_ONLY_TABS = {"smart_upload", "monthly_upload", "configuration"}


# ─────────────────────────────────────────────────────────────────────────────
# PASSWORD UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _hash_password(password: str, salt: str = None):
    """Return (hash, salt). Generates a new salt if none provided."""
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return h, salt


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    h, _ = _hash_password(password, salt)
    return h == stored_hash


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _table_exists(supabase) -> bool:
    try:
        supabase.table("app_users").select("id").limit(1).execute()
        return True
    except Exception:
        return False


def load_all_users(supabase) -> pd.DataFrame:
    try:
        res = (supabase.table("app_users")
               .select("id, username, allowed_tabs, created_at")
               .order("created_at")
               .execute())
        if not res.data:
            return pd.DataFrame(columns=["id", "username", "allowed_tabs", "created_at"])
        return pd.DataFrame(res.data)
    except Exception as e:
        st.error(f"Could not load users: {e}")
        return pd.DataFrame(columns=["id", "username", "allowed_tabs", "created_at"])


def get_user_for_login(supabase, username: str):
    try:
        res = (supabase.table("app_users")
               .select("*")
               .eq("username", username)
               .limit(1)
               .execute())
        return res.data[0] if res.data else None
    except Exception:
        return None


def create_user(supabase, username: str, password: str, allowed_tabs: list) -> bool:
    pw_hash, salt = _hash_password(password)
    try:
        supabase.table("app_users").insert({
            "username":      username,
            "password_hash": pw_hash,
            "salt":          salt,
            "allowed_tabs":  json.dumps(allowed_tabs),
        }).execute()
        return True
    except Exception as e:
        st.error(f"Failed to create user: {e}")
        return False


def update_user_tabs(supabase, user_id: int, allowed_tabs: list) -> bool:
    try:
        (supabase.table("app_users")
         .update({"allowed_tabs": json.dumps(allowed_tabs)})
         .eq("id", user_id)
         .execute())
        return True
    except Exception as e:
        st.error(f"Failed to update permissions: {e}")
        return False


def update_user_password(supabase, user_id: int, new_password: str) -> bool:
    pw_hash, salt = _hash_password(new_password)
    try:
        (supabase.table("app_users")
         .update({"password_hash": pw_hash, "salt": salt})
         .eq("id", user_id)
         .execute())
        return True
    except Exception as e:
        st.error(f"Failed to update password: {e}")
        return False


def delete_user(supabase, user_id: int) -> bool:
    try:
        supabase.table("app_users").delete().eq("id", user_id).execute()
        return True
    except Exception as e:
        st.error(f"Failed to delete user: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# ACCESS CONTROL HELPERS  (called from app.py for every tab)
# ─────────────────────────────────────────────────────────────────────────────

def is_tab_allowed(tab_key: str) -> bool:
    """
    Return True if the current session may view this tab.
    - Admin / Viewer (legacy roles)  → always True
    - Custom user (role="user")      → check allowed_tabs in session_state
    """
    role = st.session_state.get("role", "")
    if role in ("admin", "viewer"):
        return True
    if role == "user":
        return tab_key in st.session_state.get("allowed_tabs", [])
    return False

# Alias used by app.py
has_tab_access = is_tab_allowed


def load_user_session(supabase, username: str, password: str) -> bool:
    """
    Validate credentials and populate session_state on success.
    Sets: authenticated, role, username, allowed_tabs
    """
    record = get_user_for_login(supabase, username)
    if record is None:
        return False
    if not _verify_password(password, record["password_hash"], record["salt"]):
        return False

    raw = record.get("allowed_tabs", "[]")
    if isinstance(raw, str):
        try:
            allowed = json.loads(raw)
        except Exception:
            allowed = []
    elif isinstance(raw, list):
        allowed = raw
    else:
        allowed = []

    st.session_state["authenticated"] = True
    st.session_state["role"]          = "user"
    st.session_state["username"]      = username
    st.session_state["allowed_tabs"]  = allowed
    return True


def tab_denied_message(tab_name: str) -> None:
    """Render a styled 'Access Restricted' placeholder inside a locked tab."""
    st.markdown(
        f"""
        <div style="display:flex;flex-direction:column;align-items:center;
                    justify-content:center;padding:5rem 2rem;text-align:center;">
          <div style="font-size:3rem;margin-bottom:1rem;opacity:0.4;">🔒</div>
          <div style="font-family:'DM Sans',sans-serif;font-size:1.05rem;
                      font-weight:600;color:#1C1917;margin-bottom:0.5rem;">
            Access Restricted
          </div>
          <div style="font-size:0.83rem;color:#A89E95;max-width:300px;line-height:1.6;">
            You don't have permission to view
            <strong>{tab_name}</strong>.<br>
            Contact your administrator to request access.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# USER MANAGEMENT TAB UI
# ─────────────────────────────────────────────────────────────────────────────

def render_user_management_tab(supabase) -> None:
    """
    Full User Management UI — admin only.
    Provides: add user, edit permissions, change password, delete user.
    """
    if st.session_state.get("role") != "admin":
        tab_denied_message("User Management")
        return

    # Page header
    st.markdown(
        """
        <div style="margin-bottom:1.5rem;">
          <div style="font-family:'DM Serif Display',Georgia,serif;
                      font-size:1.6rem;color:#1C1917;margin-bottom:0.25rem;">
            User Management
          </div>
          <div style="font-size:0.82rem;color:#A89E95;">
            Create viewer accounts with per-tab access control. Maximum 10 users.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Table check ───────────────────────────────────────────────────────────
    if not _table_exists(supabase):
        st.error("The `app_users` table is missing from your Supabase database.")
        st.markdown("**Create it by running this SQL in your Supabase SQL Editor:**")
        st.code("""
CREATE TABLE IF NOT EXISTS app_users (
  id            SERIAL PRIMARY KEY,
  username      TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  salt          TEXT NOT NULL,
  allowed_tabs  JSONB DEFAULT '[]'::jsonb,
  created_at    TIMESTAMPTZ DEFAULT now()
);
        """, language="sql")
        return

    users_df    = load_all_users(supabase)
    user_count  = len(users_df)
    grantable   = [(k, l) for k, l in ALL_TABS if k not in ADMIN_ONLY_TABS]

    # ── KPI row ───────────────────────────────────────────────────────────────
    m1, m2, m3 = st.columns(3)
    m1.metric("Users Created", f"{user_count} / {MAX_USERS}")
    m2.metric("Slots Remaining", MAX_USERS - user_count)
    m3.metric("Grantable Tabs", len(grantable))

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # ADD NEW USER
    # ════════════════════════════════════════════════════════════════════════
    st.markdown(
        '<p style="font-size:0.78rem;font-weight:700;color:#6B5F55;'
        'text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.6rem;">'
        '➕ Create New User</p>',
        unsafe_allow_html=True,
    )

    if user_count >= MAX_USERS:
        st.warning(f"Maximum of {MAX_USERS} users reached. Delete an existing user to add more.")
    else:
        with st.form("add_user_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                new_uname = st.text_input(
                    "Username *",
                    placeholder="letters, numbers, underscores (min 3)",
                )
            with fc2:
                new_pw = st.text_input(
                    "Password *",
                    type="password",
                    placeholder="Minimum 6 characters",
                )

            st.markdown(
                '<p style="font-size:0.72rem;font-weight:700;color:#A89E95;'
                'text-transform:uppercase;letter-spacing:0.08em;margin:0.6rem 0 0.3rem;">'
                'Tab Access Permissions</p>',
                unsafe_allow_html=True,
            )
            tab_cols  = st.columns(3)
            new_perms = []
            for i, (tab_key, tab_label) in enumerate(grantable):
                if tab_cols[i % 3].checkbox(tab_label, value=False, key=f"new_{tab_key}"):
                    new_perms.append(tab_key)

            if st.form_submit_button("✅ Create User", use_container_width=True):
                uname = new_uname.strip()
                errs  = []
                if not uname:
                    errs.append("Username is required.")
                elif len(uname) < 3:
                    errs.append("Username must be at least 3 characters.")
                elif not re.match(r"^[A-Za-z0-9_]+$", uname):
                    errs.append("Username: letters, numbers and underscores only.")
                elif not users_df.empty and uname in users_df["username"].values:
                    errs.append(f"Username '{uname}' already exists.")
                if not new_pw:
                    errs.append("Password is required.")
                elif len(new_pw) < 6:
                    errs.append("Password must be at least 6 characters.")

                if errs:
                    for e in errs:
                        st.error(e)
                elif create_user(supabase, uname, new_pw, new_perms):
                    st.success(
                        f"✅ User **{uname}** created with access to "
                        f"{len(new_perms)} tab(s)."
                    )
                    st.rerun()

    st.divider()

    # ════════════════════════════════════════════════════════════════════════
    # EXISTING USERS
    # ════════════════════════════════════════════════════════════════════════
    st.markdown(
        '<p style="font-size:0.78rem;font-weight:700;color:#6B5F55;'
        'text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.6rem;">'
        f'👥 Existing Users ({user_count})</p>',
        unsafe_allow_html=True,
    )

    if users_df.empty:
        st.info("No custom users yet. Use the form above to create one.")
        return

    for _, row in users_df.iterrows():
        uid      = int(row["id"])
        uname    = row["username"]
        created  = str(row.get("created_at", ""))[:10]

        raw      = row.get("allowed_tabs", "[]")
        if isinstance(raw, str):
            try:
                cur_tabs = set(json.loads(raw))
            except Exception:
                cur_tabs = set()
        elif isinstance(raw, list):
            cur_tabs = set(raw)
        else:
            cur_tabs = set()

        n_tabs = len(cur_tabs)

        with st.expander(
            f"👤 **{uname}**  ·  {n_tabs} tab(s) allowed  ·  added {created}"
        ):
            left_col, right_col = st.columns([3, 1])

            # ── Permissions ───────────────────────────────────────────────
            with left_col:
                st.markdown(
                    '<p style="font-size:0.72rem;font-weight:700;color:#A89E95;'
                    'text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.4rem;">'
                    'Tab Permissions</p>',
                    unsafe_allow_html=True,
                )
                pcols      = st.columns(3)
                upd_tabs   = []
                for i, (tab_key, tab_label) in enumerate(grantable):
                    if pcols[i % 3].checkbox(
                        tab_label,
                        value=(tab_key in cur_tabs),
                        key=f"perm_{uid}_{tab_key}",
                    ):
                        upd_tabs.append(tab_key)

                if st.button("💾 Save Permissions", key=f"save_{uid}"):
                    if update_user_tabs(supabase, uid, upd_tabs):
                        st.success("Permissions saved.")
                        st.rerun()

            # ── Password + Delete ─────────────────────────────────────────
            with right_col:
                st.markdown(
                    '<p style="font-size:0.72rem;font-weight:700;color:#A89E95;'
                    'text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.4rem;">'
                    'Change Password</p>',
                    unsafe_allow_html=True,
                )
                new_pw_val = st.text_input(
                    "New password",
                    type="password",
                    key=f"pw_{uid}",
                    label_visibility="collapsed",
                    placeholder="New password…",
                )
                if st.button("🔑 Update", key=f"pw_btn_{uid}"):
                    if not new_pw_val or len(new_pw_val) < 6:
                        st.error("Min 6 characters.")
                    elif update_user_password(supabase, uid, new_pw_val):
                        st.success("Password updated.")

                st.markdown(
                    '<p style="font-size:0.72rem;font-weight:700;color:#B91C1C;'
                    'text-transform:uppercase;letter-spacing:0.07em;margin:0.75rem 0 0.4rem;">'
                    'Delete User</p>',
                    unsafe_allow_html=True,
                )
                if st.checkbox("Confirm delete", key=f"cdel_{uid}"):
                    if st.button(f"🗑️ Delete '{uname}'", key=f"del_{uid}"):
                        if delete_user(supabase, uid):
                            st.success(f"User '{uname}' deleted.")
                            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# LOGIN HELPER  (used by check_auth in app.py)
# ─────────────────────────────────────────────────────────────────────────────

def render_user_login_option(supabase) -> bool:
    """
    Renders the custom-user login fields.
    Returns True if login succeeded (caller should st.rerun()).
    """
    uname = st.text_input(
        "Username",
        placeholder="Your username",
        label_visibility="collapsed",
        key="ul_username",
    )
    pw = st.text_input(
        "Password",
        type="password",
        placeholder="Your password",
        label_visibility="collapsed",
        key="ul_password",
    )
    if st.button("Sign In →", use_container_width=True, key="ul_signin"):
        if not uname.strip() or not pw:
            st.error("Please enter both username and password.")
            return False
        if load_user_session(supabase, uname.strip(), pw):
            return True
        st.error("Invalid username or password.")
    return False


# Alias for backward compatibility with app.py imports
authenticate_custom_user = load_user_session
