--liquibase formatted sql

--changeset 60098727:grant:trackandtrace_marts:defaults runAlways:true runOnChange:true
SELECT public.fn_grant_on_all_objects('trackandtrace_marts', 'r', 'trackandtrace_marts_r');

--changeset 60098727:grant:trackandtrace_marts_rwx runOnChange:true

GRANT trackandtrace_marts_rwx TO trackandtrace_marts_etlbot;

--changeset 60098727:grant:trackandtrace_marts_owner runOnChange:true

GRANT trackandtrace_marts_owner TO dba;

--changeset 60098727:grant:trackandtrace_marts_r runOnChange:true

GRANT trackandtrace_marts_r TO "lm-dataread-public";

--changeset 60098727:grant:role:trackandtrace_marts_r_cfo_marts_etlbot runOnChange:true

GRANT trackandtrace_marts_r TO "cfo_marts_etlbot";

--changeset 60084096:grant:changelog_reader

GRANT SELECT ON schemachangelog, schemachangeloglock TO changelog_reader;
