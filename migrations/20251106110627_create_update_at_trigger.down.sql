-- Remove updated_at trigger
DROP TRIGGER IF EXISTS update_storage_providers_updated_at ON storage_providers;
DROP FUNCTION IF EXISTS update_updated_at_column();
