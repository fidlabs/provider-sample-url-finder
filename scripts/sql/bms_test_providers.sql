-- BMS Test Providers Seed
-- Providers: f08403, f01518369
-- Real deal data from sample_deals.sql and providers_with_clients.sql

-- Insert sample deals for test providers
INSERT INTO unified_verified_deal (id, "dealId", "claimId", type, "clientId", "providerId",
  "sectorId", "pieceCid", "pieceSize", "termMax", "termMin", "termStart", "slashedEpoch", "processedSlashedEpoch", removed, "createdAt", "updatedAt", "dcSource")
VALUES
-- f08403 deal
(151651301, 0, 118131274, 'claim', '3200311', '8403', '21097', 'baga6ea4seaqh32ogu5iav6gzkxvdsyustxpcsn3zygunp2ohxxpuce3dxiekcgy', 34359738368, 1785600, 1526400, 5461183, 0, 0, false, '2025-11-02 22:29:01.859', '2025-11-02 22:29:01.859', NULL),
-- f01518369 deals
(61696683, 29547762, 0, 'deal', '1837711', '1518369', '643629', 'baga6ea4seaqp47n3cjm3y2sll3if7kusze4e7h2wuybpxzzv6wszbwdxftk76ki', 34359738368, 4218244, NULL, 2708343, 0, 0, false, '2024-10-23 16:21:16.512869', '2024-10-23 16:21:16.512869', NULL),
(52701111, 19334805, 0, 'deal', '1880196', '1518369', '0', 'baga6ea4seaqpkzvgyguwodcetsrh2uxak2efh62bf6vyxbdel23l73jmmjq5ypi', 34359738368, 3951512, NULL, 2438141, 0, 0, false, '2024-10-23 14:49:28.360997', '2024-10-23 14:49:28.360997', NULL),
(61696685, 29547763, 0, 'deal', '1901107', '1518369', '643628', 'baga6ea4seaqivqjo2bg2gmjqo2d6phwgyzil2hrwihhcn7kzm2gbl42ffa2nchy', 34359738368, 4220461, NULL, 2708334, 0, 0, false, '2024-10-23 16:21:16.512869', '2024-10-23 16:21:16.512869', NULL),
(110031315, 83028807, 0, 'deal', '1924648', '1518369', '722558', 'baga6ea4seaqjuhxvn33zkqvuzmjsggla35un3pohav2awpcerxefijlf4pazimi', 34359738368, 7626855, NULL, 3980991, 0, 0, false, '2024-10-23 19:58:37.739883', '2024-10-23 19:58:37.739883', NULL),
(109627694, 82129856, 0, 'deal', '1924649', '1518369', '711737', 'baga6ea4seaqmy5sfbbcye7jfchyrlkpztbfsuhhkcxir4niz33oklr2lbcfnkii', 34359738368, 7604226, NULL, 3951000, 0, 0, false, '2024-10-23 19:57:57.51002', '2024-10-23 19:57:57.51002', NULL);

-- Setting next_bms_test_at in the past so they're immediately eligible
INSERT INTO storage_providers (provider_id, next_bms_test_at, next_url_discovery_at)
VALUES
('8403', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
('1518369', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')
ON CONFLICT (provider_id) DO UPDATE SET
    next_bms_test_at = EXCLUDED.next_bms_test_at,
    next_url_discovery_at = EXCLUDED.next_url_discovery_at;
