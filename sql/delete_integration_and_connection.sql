-- Delete integration and connection for a platform

WITH deleted_integrations AS (
    DELETE FROM integration 
    WHERE platform = 'ado_boards' 
    RETURNING id
)
DELETE FROM integration_connection 
WHERE "integrationId" IN (SELECT id FROM deleted_integrations);