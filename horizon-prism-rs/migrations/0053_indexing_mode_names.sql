-- The indexing modes are named kryphocron_only, community and full.
-- Rewrite the stored values of indexing.public_scope from the old names.

UPDATE operator_settings
SET value = CASE value #>> '{}'
        WHEN 'off' THEN '"kryphocron_only"'::jsonb
        WHEN 'community_scoped' THEN '"community"'::jsonb
        ELSE value
    END
WHERE key = 'indexing.public_scope';

UPDATE operator_settings
SET file_value = jsonb_set(
        file_value,
        '{value}',
        CASE file_value->>'value'
            WHEN 'off' THEN '"kryphocron_only"'::jsonb
            ELSE '"community"'::jsonb
        END
    )
WHERE key = 'indexing.public_scope'
  AND file_value->>'value' IN ('off', 'community_scoped');

-- A switch waiting to turn back on.
UPDATE worker_state
SET value = jsonb_set(value, '{pending}', '"community"'::jsonb)
WHERE key = 'public_transition'
  AND value->>'pending' = 'community_scoped';
