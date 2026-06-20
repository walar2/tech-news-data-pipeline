CREATE TABLE IF NOT EXISTS audit.data_quality_result (
    result_key BIGSERIAL PRIMARY KEY,

    check_date DATE NOT NULL DEFAULT CURRENT_DATE,

    check_name TEXT NOT NULL,

    source_name TEXT NOT NULL
        CHECK (
            source_name IN (
                'HackerNews',
                'Dev.to',
                'all_sources'
            )
        ),

    passed BOOLEAN NOT NULL,

    observed_value NUMERIC,

    expected_value TEXT NOT NULL,

    details JSONB NOT NULL DEFAULT '{}',

    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS audit.daily_pipeline_status (
    status_date DATE PRIMARY KEY,

    hackernews_status TEXT NOT NULL
        CHECK (
            hackernews_status IN (
                'success',
                'failed',
                'missing',
                'not_configured'
            )
        ),

    devto_status TEXT NOT NULL
        CHECK (
            devto_status IN (
                'success',
                'failed',
                'missing',
                'not_configured'
            )
        ),

    quality_status TEXT NOT NULL
        CHECK (
            quality_status IN (
                'success',
                'warning',
                'failed'
            )
        ),

    message TEXT NOT NULL,

    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);