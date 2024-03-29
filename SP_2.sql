USE [UCSDC_EcoCurrent]
GO
/****** Object:  StoredProcedure [dbo].[zsp_reindex_new]    Script Date: 3/30/2024 1:18:00 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[zsp_reindex_new]
(
    @in_mode INT = 1,
    @in_maxfragment INT = 80,
    @in_debug INT = 0,
	@in_diplayindexes BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @tablestoprocess TABLE
    (
        id INT IDENTITY,
        Tablename VARCHAR(100),
        IndexName VARCHAR(250),
        ReindexMode VARCHAR(250),
        maxfragment NUMERIC(15, 6)
    );

    DECLARE @currid INT,
            @lastid INT,
            @tablename VARCHAR(100),
            @indexname VARCHAR(250);

    DECLARE @ReindexMode VARCHAR(50) = 'REORGANIZE'; --REBUILD';
    DECLARE @fragmentations INT = 80;
    DECLARE @v_dbname NVARCHAR(256);
    DECLARE @v_dbid INT;

    SET @fragmentations = @in_maxfragment;

    IF @in_mode IN (2,3)
    BEGIN
        SET @ReindexMode = 'REBUILD';
    END;
    ELSE
    BEGIN
        SET @ReindexMode = 'REORGANIZE';
    END;

    IF @in_debug > 0
    BEGIN
        SELECT @v_dbname dbname,
               @v_dbid v_dbid;
        SELECT S.name AS 'Schema',
               T.name AS 'TableName',
               I.name AS 'IndexName',
               DDIPS.avg_fragmentation_in_percent,
               DDIPS.page_count
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS DDIPS
            INNER JOIN sys.tables T
                ON T.object_id = DDIPS.object_id
            INNER JOIN sys.schemas S
                ON T.schema_id = S.schema_id
            INNER JOIN sys.indexes I
                ON I.object_id = DDIPS.object_id
                   AND DDIPS.index_id = I.index_id
        WHERE DDIPS.database_id = DB_ID()
              AND I.name IS NOT NULL
              AND DDIPS.avg_fragmentation_in_percent >= @fragmentations;
    END;

    DECLARE @RecordCount INT;
    DECLARE @v_sql NVARCHAR(250);
    INSERT INTO @tablestoprocess
    (
        Tablename,
        IndexName,
		ReindexMode,
        maxfragment
    )
    SELECT TableName,
           IndexName,
		   @ReindexMode,
           CAST(MAX(avg_fragmentation_in_percent) AS NUMERIC(15, 6)) MaxFragment
    FROM
    (
        SELECT S.name AS 'Schema',
               T.name AS 'TableName',
               I.name AS 'IndexName',
               DDIPS.avg_fragmentation_in_percent,
               DDIPS.page_count
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS DDIPS
            INNER JOIN sys.tables T
                ON T.object_id = DDIPS.object_id
            INNER JOIN sys.schemas S
                ON T.schema_id = S.schema_id
            INNER JOIN sys.indexes I
                ON I.object_id = DDIPS.object_id
                   AND DDIPS.index_id = I.index_id
        WHERE DDIPS.database_id = DB_ID()
              AND I.name IS NOT NULL
              AND DDIPS.avg_fragmentation_in_percent >= @fragmentations
    -- ORDER BY 4 desc
    ) a
    GROUP BY TableName,
             IndexName
    ORDER BY TableName;

	IF @in_mode = 3
	BEGIN 
	   INSERT INTO @tablestoprocess
		(
			Tablename,
			IndexName,
			ReindexMode,
			maxfragment
		)
		SELECT TableName,
			   IndexName,
			   'REORGANIZE' 'ReindexMode',
			   CAST(MAX(avg_fragmentation_in_percent) AS NUMERIC(15, 6)) MaxFragment
		FROM
		(
			SELECT S.name AS 'Schema',
				   T.name AS 'TableName',
				   I.name AS 'IndexName',
				   DDIPS.avg_fragmentation_in_percent,
				   DDIPS.page_count
			FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS DDIPS
				INNER JOIN sys.tables T
					ON T.object_id = DDIPS.object_id
				INNER JOIN sys.schemas S
					ON T.schema_id = S.schema_id
				INNER JOIN sys.indexes I
					ON I.object_id = DDIPS.object_id
					   AND DDIPS.index_id = I.index_id
			WHERE DDIPS.database_id = DB_ID()
				  AND I.name IS NOT NULL
				  AND DDIPS.avg_fragmentation_in_percent > 0
				  AND DDIPS.avg_fragmentation_in_percent < @fragmentations

		-- ORDER BY 4 desc
		) a
		GROUP BY TableName,
				 IndexName
		ORDER BY TableName;
	END

    SELECT @RecordCount = @@Rowcount;
    PRINT 'Indexes to be ' + CAST(@ReindexMode AS VARCHAR) + ': ' +  CAST(@RecordCount AS VARCHAR);

    SELECT @currid = MIN(id),
           @lastid = MAX(id)
    FROM @tablestoprocess;
    WHILE @currid <= @lastid
    BEGIN
        SELECT @tablename = Tablename,
               @indexname = IndexName,
			   @ReindexMode = ReindexMode
        FROM @tablestoprocess
        WHERE id = @currid;


        --DBCC DBREINDEX(@tablename);
        SET @v_sql = ' alter index ' + @indexname + ' ON ' + @tablename + ' ' + @ReindexMode;
        BEGIN TRY
            EXEC dbo.sp_executesql @v_sql;
			IF @in_diplayindexes = 1 
			begin
            PRINT @v_sql;
			end
        END TRY
        BEGIN CATCH
            PRINT 'Error in the alter index statement ' + ERROR_MESSAGE();
            PRINT @v_sql;
        END CATCH;
        SET @currid = @currid + 1;

    END;



END;