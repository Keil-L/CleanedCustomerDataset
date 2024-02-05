
-- Start transaction for data integrity
BEGIN TRY
    BEGIN TRANSACTION;

    -- Step 1: Remove records with invalid or null email addresses
    DELETE FROM customer_data
    WHERE email IS NULL OR email NOT LIKE '%_@__%.__%'; -- Simple pattern check for email validity

    -- Step 2: Standardize phone number format (assuming format: +1234567890)
    UPDATE customer_data
    SET phone = CONCAT('+', REPLACE(REPLACE(REPLACE(phone, '-', ''), ' ', ''), '(', ''), ')', '')
    WHERE phone IS NOT NULL;

    -- Step 3: Trim extra spaces from name and address fields
    UPDATE customer_data
    SET 
        name = LTRIM(RTRIM(name)),
        address = LTRIM(RTRIM(address))
    WHERE name IS NOT NULL OR address IS NOT NULL;

    -- Step 4: Set default values for null registration dates
    UPDATE customer_data
    SET registration_date = GETDATE() -- Current date as default
    WHERE registration_date IS NULL;

    -- Step 5: Deduplicate records (keep the latest based on id)
    ;WITH DuplicateRecords AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY email ORDER BY id DESC) AS rn
        FROM customer_data
    )
    DELETE FROM DuplicateRecords WHERE rn > 1;

    -- Step 6: Log the changes (assuming a logging table `data_cleaning_log`)
    INSERT INTO data_cleaning_log (operation, table_name, timestamp)
    VALUES ('CLEANING', 'customer_data', CURRENT_TIMESTAMP);

    -- Commit the transaction
    COMMIT TRANSACTION;
    PRINT 'Data cleaning process completed successfully.';
END TRY
BEGIN CATCH
    -- Rollback transaction in case of an error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Error handling
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();

    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH

-- End of the script
