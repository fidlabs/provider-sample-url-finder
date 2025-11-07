-- Create reusable trigger function for updating updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_storage_providers_updated_at
    BEFORE UPDATE ON storage_providers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
