INSERT INTO sync_source_db (
    db_key,
    name,
    jdbc_url,
    username,
    password_secret_key,
    driver_class_name,
    enabled
)
VALUES (
    'sample-source-db',
    'Sample Source DB',
    'jdbc:postgresql://localhost:5432/source_db',
    'postgres',
    'PGadmin11**',
    'org.postgresql.Driver',
    TRUE
)
ON CONFLICT (db_key) DO UPDATE SET
    name = EXCLUDED.name,
    jdbc_url = EXCLUDED.jdbc_url,
    username = EXCLUDED.username,
    password_secret_key = EXCLUDED.password_secret_key,
    driver_class_name = EXCLUDED.driver_class_name,
    enabled = EXCLUDED.enabled,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO sync_source_config (
    id,
    source_db_key,
    name,
    incremental_sql,
    fetch_one_sql,
    key_alias,
    updated_at_alias,
    upsert_sql,
    chunk_size,
    fetch_size,
    safety_lag_seconds,
    overlap_seconds,
    enabled,
    schedule_enabled,
    schedule_interval_minutes,
    next_run_at,
    last_scheduled_at
)
VALUES (
    1,
    'sample-source-db',
    'legacy_user single table sample sync',
    $SQL$
SELECT
    CAST(u.user_id AS VARCHAR) AS account_key,
    u.login_id                 AS login_id,
    u.user_name                AS name,
    u.email                    AS email,
    u.phone_no                 AS phone,
    NULL                       AS address_text,
    NULL                       AS organization_name,
    CAST(NULL AS TEXT)         AS extra_payload,
    CASE WHEN u.deleted THEN 'INACTIVE' ELSE 'ACTIVE' END AS status,
    u.updated_at               AS sync_updated_at
FROM legacy_user u
WHERE (
        u.updated_at > :last_updated_at
        OR (
            u.updated_at = :last_updated_at
            AND CAST(u.user_id AS VARCHAR) > :last_account_key
        )
      )
  AND u.updated_at <= :current_cutoff
ORDER BY u.updated_at ASC, CAST(u.user_id AS VARCHAR) ASC
LIMIT :chunk_size
$SQL$,
    $SQL$
SELECT
    CAST(u.user_id AS VARCHAR) AS account_key,
    u.login_id                 AS login_id,
    u.user_name                AS name,
    u.email                    AS email,
    u.phone_no                 AS phone,
    NULL                       AS address_text,
    NULL                       AS organization_name,
    CAST(NULL AS TEXT)         AS extra_payload,
    CASE WHEN u.deleted THEN 'INACTIVE' ELSE 'ACTIVE' END AS status,
    u.updated_at               AS sync_updated_at
FROM legacy_user u
WHERE CAST(u.user_id AS VARCHAR) = :account_key
$SQL$,
    'account_key',
    'sync_updated_at',
    $SQL$
INSERT INTO target_user (
    account_key,
    login_id,
    name,
    email,
    phone,
    address_text,
    organization_name,
    extra_payload,
    status,
    source_updated_at,
    sync_mode,
    last_synced_at,
    updated_at
)
VALUES (
    :account_key,
    :login_id,
    :name,
    :email,
    :phone,
    :address_text,
    :organization_name,
    CAST(:extra_payload AS jsonb),
    :status,
    :sync_updated_at,
    'SYNC',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (account_key)
DO UPDATE SET
    login_id = EXCLUDED.login_id,
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    address_text = EXCLUDED.address_text,
    organization_name = EXCLUDED.organization_name,
    extra_payload = EXCLUDED.extra_payload,
    status = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    sync_mode = 'SYNC',
    last_synced_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE target_user.sync_mode <> 'MANUAL'
$SQL$,
    500,
    500,
    30,
    300,
    TRUE,
    FALSE,
    5,
    NULL,
    NULL
)
ON CONFLICT (id) DO UPDATE SET
    source_db_key = EXCLUDED.source_db_key,
    name = EXCLUDED.name,
    incremental_sql = EXCLUDED.incremental_sql,
    fetch_one_sql = EXCLUDED.fetch_one_sql,
    key_alias = EXCLUDED.key_alias,
    updated_at_alias = EXCLUDED.updated_at_alias,
    upsert_sql = EXCLUDED.upsert_sql,
    chunk_size = EXCLUDED.chunk_size,
    fetch_size = EXCLUDED.fetch_size,
    safety_lag_seconds = EXCLUDED.safety_lag_seconds,
    overlap_seconds = EXCLUDED.overlap_seconds,
    enabled = EXCLUDED.enabled,
    schedule_enabled = EXCLUDED.schedule_enabled,
    schedule_interval_minutes = EXCLUDED.schedule_interval_minutes,
    next_run_at = CASE
        WHEN EXCLUDED.enabled = TRUE AND EXCLUDED.schedule_enabled = TRUE
            THEN COALESCE(
                sync_source_config.next_run_at,
                CURRENT_TIMESTAMP + (EXCLUDED.schedule_interval_minutes * INTERVAL '1 minute')
            )
        ELSE NULL
    END,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO sync_source_config (
    id,
    source_db_key,
    name,
    incremental_sql,
    fetch_one_sql,
    key_alias,
    updated_at_alias,
    upsert_sql,
    chunk_size,
    fetch_size,
    safety_lag_seconds,
    overlap_seconds,
    enabled,
    schedule_enabled,
    schedule_interval_minutes,
    next_run_at,
    last_scheduled_at
)
VALUES (
    2,
    'sample-source-db',
    'split user tables sample sync',
    $SQL$
WITH changed_keys AS (
    SELECT CAST('split:' || u.user_id AS VARCHAR) AS account_key, u.updated_at AS sync_updated_at
    FROM split_user u
    WHERE u.updated_at >= :last_updated_at
      AND u.updated_at <= :current_cutoff

    UNION ALL

    SELECT CAST('split:' || p.user_id AS VARCHAR) AS account_key, p.updated_at AS sync_updated_at
    FROM split_user_phone p
    WHERE p.updated_at >= :last_updated_at
      AND p.updated_at <= :current_cutoff

    UNION ALL

    SELECT CAST('split:' || a.user_id AS VARCHAR) AS account_key, a.updated_at AS sync_updated_at
    FROM split_user_address a
    WHERE a.updated_at >= :last_updated_at
      AND a.updated_at <= :current_cutoff

    UNION ALL

    SELECT CAST('split:' || o.user_id AS VARCHAR) AS account_key, o.updated_at AS sync_updated_at
    FROM split_user_organization o
    WHERE o.updated_at >= :last_updated_at
      AND o.updated_at <= :current_cutoff
),
page_keys AS (
    SELECT
        account_key,
        MAX(sync_updated_at) AS sync_updated_at
    FROM changed_keys
    GROUP BY account_key
    HAVING (
        MAX(sync_updated_at) > :last_updated_at
        OR (
            MAX(sync_updated_at) = :last_updated_at
            AND account_key > :last_account_key
        )
    )
    ORDER BY MAX(sync_updated_at) ASC, account_key ASC
    LIMIT :chunk_size
),
phone_agg AS (
    SELECT
        user_id,
        STRING_AGG(phone_no, ', ' ORDER BY is_primary DESC, phone_id ASC) AS phone
    FROM split_user_phone
    WHERE deleted = FALSE
    GROUP BY user_id
),
address_one AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        CONCAT_WS(' ', zip_code, address1, address2) AS address_text
    FROM split_user_address
    WHERE deleted = FALSE
    ORDER BY user_id, is_primary DESC, address_id ASC
),
org_one AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        org_name
    FROM split_user_organization
    WHERE deleted = FALSE
    ORDER BY user_id, is_primary DESC, organization_id ASC
)
SELECT
    pk.account_key              AS account_key,
    u.login_id                  AS login_id,
    u.user_name                 AS name,
    u.email                     AS email,
    pa.phone                    AS phone,
    ao.address_text             AS address_text,
    oo.org_name                 AS organization_name,
    CASE WHEN u.deleted THEN 'INACTIVE' ELSE 'ACTIVE' END AS status,
    pk.sync_updated_at          AS sync_updated_at,
    jsonb_build_object(
        'phones', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('type', p2.phone_type, 'phone', p2.phone_no)
                ORDER BY p2.is_primary DESC, p2.phone_id ASC
            )
            FROM split_user_phone p2
            WHERE p2.user_id = u.user_id
              AND p2.deleted = FALSE
        ), '[]'::jsonb),
        'addresses', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('zip_code', a2.zip_code, 'address1', a2.address1, 'address2', a2.address2)
                ORDER BY a2.is_primary DESC, a2.address_id ASC
            )
            FROM split_user_address a2
            WHERE a2.user_id = u.user_id
              AND a2.deleted = FALSE
        ), '[]'::jsonb),
        'organization', oo.org_name
    )::text                     AS extra_payload
FROM page_keys pk
JOIN split_user u
  ON CAST('split:' || u.user_id AS VARCHAR) = pk.account_key
LEFT JOIN phone_agg pa
  ON pa.user_id = u.user_id
LEFT JOIN address_one ao
  ON ao.user_id = u.user_id
LEFT JOIN org_one oo
  ON oo.user_id = u.user_id
ORDER BY pk.sync_updated_at ASC, pk.account_key ASC
$SQL$,
    $SQL$
WITH phone_agg AS (
    SELECT
        user_id,
        STRING_AGG(phone_no, ', ' ORDER BY is_primary DESC, phone_id ASC) AS phone
    FROM split_user_phone
    WHERE deleted = FALSE
    GROUP BY user_id
),
phone_max AS (
    SELECT user_id, MAX(updated_at) AS phone_max_updated_at
    FROM split_user_phone
    GROUP BY user_id
),
address_one AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        CONCAT_WS(' ', zip_code, address1, address2) AS address_text,
        updated_at AS address_updated_at
    FROM split_user_address
    WHERE deleted = FALSE
    ORDER BY user_id, is_primary DESC, address_id ASC
),
address_max AS (
    SELECT user_id, MAX(updated_at) AS address_max_updated_at
    FROM split_user_address
    GROUP BY user_id
),
org_one AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        org_name,
        updated_at AS org_updated_at
    FROM split_user_organization
    WHERE deleted = FALSE
    ORDER BY user_id, is_primary DESC, organization_id ASC
),
org_max AS (
    SELECT user_id, MAX(updated_at) AS org_max_updated_at
    FROM split_user_organization
    GROUP BY user_id
)
SELECT
    CAST('split:' || u.user_id AS VARCHAR) AS account_key,
    u.login_id                  AS login_id,
    u.user_name                 AS name,
    u.email                     AS email,
    pa.phone                    AS phone,
    ao.address_text             AS address_text,
    oo.org_name                 AS organization_name,
    CASE WHEN u.deleted THEN 'INACTIVE' ELSE 'ACTIVE' END AS status,
    GREATEST(
        u.updated_at,
        COALESCE(pm.phone_max_updated_at, TIMESTAMP '1970-01-01 00:00:00'),
        COALESCE(am.address_max_updated_at, TIMESTAMP '1970-01-01 00:00:00'),
        COALESCE(om.org_max_updated_at, TIMESTAMP '1970-01-01 00:00:00')
    )                            AS sync_updated_at,
    jsonb_build_object(
        'phones', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('type', p2.phone_type, 'phone', p2.phone_no)
                ORDER BY p2.is_primary DESC, p2.phone_id ASC
            )
            FROM split_user_phone p2
            WHERE p2.user_id = u.user_id
              AND p2.deleted = FALSE
        ), '[]'::jsonb),
        'addresses', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object('zip_code', a2.zip_code, 'address1', a2.address1, 'address2', a2.address2)
                ORDER BY a2.is_primary DESC, a2.address_id ASC
            )
            FROM split_user_address a2
            WHERE a2.user_id = u.user_id
              AND a2.deleted = FALSE
        ), '[]'::jsonb),
        'organization', oo.org_name
    )::text                      AS extra_payload
FROM split_user u
LEFT JOIN phone_agg pa
  ON pa.user_id = u.user_id
LEFT JOIN phone_max pm
  ON pm.user_id = u.user_id
LEFT JOIN address_one ao
  ON ao.user_id = u.user_id
LEFT JOIN address_max am
  ON am.user_id = u.user_id
LEFT JOIN org_one oo
  ON oo.user_id = u.user_id
LEFT JOIN org_max om
  ON om.user_id = u.user_id
WHERE CAST('split:' || u.user_id AS VARCHAR) = :account_key
$SQL$,
    'account_key',
    'sync_updated_at',
    $SQL$
INSERT INTO target_user (
    account_key,
    login_id,
    name,
    email,
    phone,
    address_text,
    organization_name,
    extra_payload,
    status,
    source_updated_at,
    sync_mode,
    last_synced_at,
    updated_at
)
VALUES (
    :account_key,
    :login_id,
    :name,
    :email,
    :phone,
    :address_text,
    :organization_name,
    CAST(:extra_payload AS jsonb),
    :status,
    :sync_updated_at,
    'SYNC',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (account_key)
DO UPDATE SET
    login_id = EXCLUDED.login_id,
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    address_text = EXCLUDED.address_text,
    organization_name = EXCLUDED.organization_name,
    extra_payload = EXCLUDED.extra_payload,
    status = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    sync_mode = 'SYNC',
    last_synced_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE target_user.sync_mode <> 'MANUAL'
$SQL$,
    500,
    500,
    30,
    300,
    TRUE,
    FALSE,
    5,
    NULL,
    NULL
)
ON CONFLICT (id) DO UPDATE SET
    source_db_key = EXCLUDED.source_db_key,
    name = EXCLUDED.name,
    incremental_sql = EXCLUDED.incremental_sql,
    fetch_one_sql = EXCLUDED.fetch_one_sql,
    key_alias = EXCLUDED.key_alias,
    updated_at_alias = EXCLUDED.updated_at_alias,
    upsert_sql = EXCLUDED.upsert_sql,
    chunk_size = EXCLUDED.chunk_size,
    fetch_size = EXCLUDED.fetch_size,
    safety_lag_seconds = EXCLUDED.safety_lag_seconds,
    overlap_seconds = EXCLUDED.overlap_seconds,
    enabled = EXCLUDED.enabled,
    schedule_enabled = EXCLUDED.schedule_enabled,
    schedule_interval_minutes = EXCLUDED.schedule_interval_minutes,
    next_run_at = CASE
        WHEN EXCLUDED.enabled = TRUE AND EXCLUDED.schedule_enabled = TRUE
            THEN COALESCE(
                sync_source_config.next_run_at,
                CURRENT_TIMESTAMP + (EXCLUDED.schedule_interval_minutes * INTERVAL '1 minute')
            )
        ELSE NULL
    END,
    updated_at = CURRENT_TIMESTAMP;

SELECT setval('sync_source_config_id_seq', GREATEST((SELECT MAX(id) FROM sync_source_config), 1));
