-- ============================================================================
-- TWO MOON ECOSYSTEM — COCKROACHDB / POSTGRESQL MASTER SCHEMA
-- Version: 1.0.0
-- Compatibility: CockroachDB Serverless >= 23.1 / PostgreSQL >= 15
-- ============================================================================

-- ============================================================================
-- DOMAIN 1: HOLLOW ENGINE — FLEET REGISTRY & DYNAMIC CONFIGURATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS bots (
    bot_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    guild_id            VARCHAR(20) NOT NULL,
    discord_client_id   VARCHAR(20) UNIQUE NOT NULL,
    name                VARCHAR(64) NOT NULL,
    bot_type            VARCHAR(32) NOT NULL,
    token_cipher        TEXT NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bots_guild_active
    ON bots (guild_id, is_active);

CREATE TABLE IF NOT EXISTS bot_configs (
    config_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_id              UUID NOT NULL REFERENCES bots (bot_id) ON DELETE CASCADE,
    guild_id            VARCHAR(20) NOT NULL,
    openrouter_api_key  TEXT,
    ai_system_prompt    TEXT,
    ai_model_id         VARCHAR(128) DEFAULT 'deepseek/deepseek-chat',
    log_channel_id      VARCHAR(20),
    feature_flags       JSONB NOT NULL DEFAULT '{}'::jsonb,
    moderation_config   JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (bot_id, guild_id)
);

CREATE INDEX IF NOT EXISTS idx_bot_configs_guild
    ON bot_configs (guild_id);

CREATE TABLE IF NOT EXISTS moderation_rules (
    rule_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_id              UUID NOT NULL REFERENCES bots (bot_id) ON DELETE CASCADE,
    guild_id            VARCHAR(20) NOT NULL,
    rule_name           VARCHAR(64) NOT NULL,
    rule_type           VARCHAR(32) NOT NULL DEFAULT 'REGEX',
    pattern             TEXT NOT NULL,
    punishment_tier     VARCHAR(16) NOT NULL DEFAULT 'WARN',
    strike_duration_sec INT NOT NULL DEFAULT 86400,
    is_enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order          INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (bot_id, guild_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_mod_rules_active
    ON moderation_rules (bot_id, guild_id, is_enabled)
    WHERE is_enabled = TRUE;

CREATE TABLE IF NOT EXISTS moderation_strikes (
    strike_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    guild_id            VARCHAR(20) NOT NULL,
    user_id             VARCHAR(20) NOT NULL,
    rule_id             UUID REFERENCES moderation_rules (rule_id) ON DELETE SET NULL,
    moderator_type      VARCHAR(16) NOT NULL DEFAULT 'AI',
    action_taken        VARCHAR(16) NOT NULL,
    reason              TEXT,
    expires_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_strikes_user_active
    ON moderation_strikes (guild_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_strikes_expiry
    ON moderation_strikes (expires_at)
    WHERE expires_at IS NOT NULL;

-- ============================================================================
-- DOMAIN 2: GUILD SETTINGS — PER-SERVER LEVELING CONFIGURATION
-- ============================================================================

CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id            VARCHAR(20) PRIMARY KEY,
    log_channel_id      VARCHAR(20),
    announce_channel_id VARCHAR(20),
    announce_enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    level_base          INT NOT NULL DEFAULT 100,
    level_exponent      REAL NOT NULL DEFAULT 1.5,
    msg_xp_min          INT NOT NULL DEFAULT 15,
    msg_xp_max          INT NOT NULL DEFAULT 25,
    msg_cooldown_sec    INT NOT NULL DEFAULT 60,
    react_xp            INT NOT NULL DEFAULT 5,
    react_cooldown_sec  INT NOT NULL DEFAULT 30,
    voice_xp_per_min    INT NOT NULL DEFAULT 10,
    swap_role_id        VARCHAR(20),
    feature_overrides   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- DOMAIN 3: TWO PATHS — FACTION SYSTEM
-- ============================================================================

CREATE TABLE IF NOT EXISTS faction_config (
    guild_id            VARCHAR(20) NOT NULL,
    faction             VARCHAR(1) NOT NULL,
    base_role_id        VARCHAR(20) NOT NULL,
    promotion_rules     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (guild_id, faction)
);

CREATE TABLE IF NOT EXISTS user_factions (
    guild_id            VARCHAR(20) NOT NULL,
    user_id             VARCHAR(20) NOT NULL,
    faction             VARCHAR(1) NOT NULL,
    faction_status      VARCHAR(16) NOT NULL DEFAULT 'VISITOR',
    total_active_days   INT NOT NULL DEFAULT 0,
    artemisium          BIGINT NOT NULL DEFAULT 0,
    last_active_date    DATE,
    joined_faction_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (guild_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_factions_status
    ON user_factions (guild_id, faction, faction_status);

-- ============================================================================
-- DOMAIN 4: LEVELING ENGINE — XP, LEVELS, REWARDS
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_levels (
    guild_id            VARCHAR(20) NOT NULL,
    user_id             VARCHAR(20) NOT NULL,
    faction             VARCHAR(1) NOT NULL,
    xp                  BIGINT NOT NULL DEFAULT 0,
    level               INT NOT NULL DEFAULT 0,
    total_messages      INT NOT NULL DEFAULT 0,
    total_reactions     INT NOT NULL DEFAULT 0,
    total_voice_minutes INT NOT NULL DEFAULT 0,
    last_msg_xp_at      TIMESTAMPTZ,
    last_react_xp_at    TIMESTAMPTZ,
    current_reward_role VARCHAR(20),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (guild_id, user_id, faction)
);

CREATE INDEX IF NOT EXISTS idx_user_levels_leaderboard
    ON user_levels (guild_id, faction, xp DESC);

CREATE INDEX IF NOT EXISTS idx_user_levels_level
    ON user_levels (guild_id, faction, level DESC);

CREATE TABLE IF NOT EXISTS role_rewards (
    guild_id            VARCHAR(20) NOT NULL,
    faction             VARCHAR(1) NOT NULL,
    level               INT NOT NULL,
    role_id             VARCHAR(20) NOT NULL,

    PRIMARY KEY (guild_id, faction, level)
);

CREATE TABLE IF NOT EXISTS blocked_roles (
    guild_id            VARCHAR(20) NOT NULL,
    role_id             VARCHAR(20) NOT NULL,

    PRIMARY KEY (guild_id, role_id)
);

CREATE TABLE IF NOT EXISTS blocked_channels (
    guild_id            VARCHAR(20) NOT NULL,
    channel_id          VARCHAR(20) NOT NULL,

    PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS boost_config (
    guild_id            VARCHAR(20) NOT NULL,
    target_id           VARCHAR(20) NOT NULL,
    target_type         VARCHAR(10) NOT NULL,
    multiplier          REAL NOT NULL DEFAULT 1.5,

    PRIMARY KEY (guild_id, target_id)
);

CREATE TABLE IF NOT EXISTS voice_sessions (
    guild_id            VARCHAR(20) NOT NULL,
    user_id             VARCHAR(20) NOT NULL,
    channel_id          VARCHAR(20) NOT NULL,
    joined_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_muted            BOOLEAN NOT NULL DEFAULT FALSE,
    muted_at            TIMESTAMPTZ,

    PRIMARY KEY (guild_id, user_id)
);

-- ============================================================================
-- DOMAIN 5: VOUCH SYSTEM
-- ============================================================================

CREATE TABLE IF NOT EXISTS vouch_codes (
    code                VARCHAR(32) PRIMARY KEY,
    guild_id            VARCHAR(20) NOT NULL,
    role_id             VARCHAR(20) NOT NULL,
    creator_id          VARCHAR(20) NOT NULL,
    rep_value           INT NOT NULL DEFAULT 0,
    status              VARCHAR(12) NOT NULL DEFAULT 'ACTIVE',
    used_by             VARCHAR(20),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_vouch_creator
    ON vouch_codes (creator_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_vouch_status
    ON vouch_codes (guild_id, status)
    WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS user_profiles (
    guild_id            VARCHAR(20) NOT NULL,
    user_id             VARCHAR(20) NOT NULL,
    reputation          INT NOT NULL DEFAULT 0,
    voucher_id          VARCHAR(20),
    first_redeemed_at   TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (guild_id, user_id)
);

-- ============================================================================
-- DOMAIN 6: WEB HUB — AUDIT LOG
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
    log_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    guild_id            VARCHAR(20) NOT NULL,
    actor_id            VARCHAR(20) NOT NULL,
    action              VARCHAR(64) NOT NULL,
    target_type         VARCHAR(32),
    target_id           VARCHAR(64),
    details             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_guild_time
    ON audit_log (guild_id, created_at DESC);
