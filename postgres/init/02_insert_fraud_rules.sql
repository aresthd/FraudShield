INSERT INTO fraud_rules 
(rule_id, rule_name, rule_type, description, threshold, window_seconds, max_transactions, score, severity, enabled, action, version, updated_at, updated_by, operation)
VALUES 
('R001', 'High Amount Transaction', 'HIGH_AMOUNT', 'Flag transaction with amount above threshold', 100000000, NULL, NULL, 40, 'HIGH', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R002', 'New Device', 'NEW_DEVICE', 'Flag transaction performed using a new device', NULL, NULL, NULL, 15, 'MEDIUM', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R003', 'New Location', 'NEW_LOCATION', 'Flag transaction performed from a new location', NULL, NULL, NULL, 15, 'MEDIUM', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R004', 'Night Transaction', 'NIGHT_TRANSACTION', 'Flag transaction performed during unusual night hours', NULL, NULL, NULL, 10, 'MEDIUM', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R005', 'Transaction Velocity', 'VELOCITY', 'More than 5 transactions within 30 seconds', NULL, 30, 5, 40, 'CRITICAL', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R006', 'Foreign Transaction', 'FOREIGN_TRANSACTION', 'Flag transaction performed outside the customer''s home country', NULL, NULL, NULL, 20, 'HIGH', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R007', 'Impossible Travel', 'IMPOSSIBLE_TRAVEL', 'Flag transaction when customer moves between distant locations in an unrealistic time', NULL, NULL, NULL, 30, 'CRITICAL', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT'),
('R008', 'High Merchant Risk', 'HIGH_MERCHANT_RISK', 'Flag transaction performed at a high-risk merchant', NULL, NULL, NULL, 20, 'HIGH', TRUE, 'FLAG', 1, CURRENT_TIMESTAMP, 'admin', 'UPSERT');