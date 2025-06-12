using Microsoft.Data.SqlClient;
using MSSQL.Copier.Server.Models;
using System.Diagnostics;

namespace MSSQL.Copier.Server.Services;

public class DatabaseService
{
    private CancellationTokenSource? _cancellationTokenSource;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly object _lockObject = new();

    private class TableInfo
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string FullName => $"[{Schema}].[{Name}]";
    }

    public DatabaseService()
    {
        // Initialize with a new token source
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public void CancelCopy()
    {
        lock (_lockObject)
        {
            _cancellationTokenSource?.Cancel();
        }
    }

    private void UpdateConnectionStatus(SqlConnection connection, ConnectionStatus status)
    {
        status.IsConnected = connection.State == System.Data.ConnectionState.Open;
        status.State = connection.State.ToString();
        status.Database = connection.Database;
        status.Server = connection.DataSource;
    }

    private async Task EnsureConnectionOpenAsync(SqlConnection connection, ConnectionStatus status, CancellationToken cancellationToken)
    {
        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (connection.State != System.Data.ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken);
            }
            UpdateConnectionStatus(connection, status);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async Task<CopyProgress> CopyDatabaseAsync(DatabaseConfig config, Func<CopyProgress, Task> onProgressUpdate, CopyProgress? existingProgress = null)
    {
        var progress = existingProgress ?? new CopyProgress();
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        
        SqlConnection? sourceConnection = null;
        SqlConnection? destinationConnection = null;

        try
        {
            // Create a new token source for this copy operation
            lock (_lockObject)
            {
                if (_cancellationTokenSource?.IsCancellationRequested == true)
                {
                    _cancellationTokenSource.Dispose();
                }
                _cancellationTokenSource = new CancellationTokenSource();
            }

            progress.Logs.Add("Starting database copy operation...");
            await onProgressUpdate(progress);

            sourceConnection = new SqlConnection(config.ConnectionString);
            await EnsureConnectionOpenAsync(sourceConnection, progress.SourceConnection, _cancellationTokenSource.Token);
            progress.Logs.Add("Connected to source database");
            await onProgressUpdate(progress);

            // Get list of tables
            var tables = await GetTablesAsync(sourceConnection, config.DatabaseName, _cancellationTokenSource.Token);
            
            // If we're resuming or retrying, filter the tables accordingly
            if (existingProgress != null)
            {
                if (existingProgress.FailedTables.Count > 0)
                {
                    // Retry mode - only process failed tables
                    tables = tables.Where(t => existingProgress.FailedTables.ContainsKey(t.FullName)).ToList();
                    progress.Logs.Add($"Retrying {tables.Count} failed tables");
                }
                else
                {
                    // Resume mode - skip completed tables
                    var completedTables = existingProgress.TableRowCounts.Keys.ToHashSet();
                    tables = tables.Where(t => !completedTables.Contains(t.FullName) || 
                                             existingProgress.CurrentTable == t.FullName).ToList();
                    progress.Logs.Add($"Resuming with {tables.Count} remaining tables");
                }
            }

            progress.TotalTables = tables.Count;
            progress.Logs.Add($"Found {tables.Count} tables to process");
            await onProgressUpdate(progress);

            // Setup destination connection
            var destinationConnectionString = config.IsLocalDestination
                ? config.ConnectionString
                : config.DestinationConnectionString;

            destinationConnection = new SqlConnection(destinationConnectionString);
            await EnsureConnectionOpenAsync(destinationConnection, progress.DestinationConnection, _cancellationTokenSource.Token);
            progress.Logs.Add("Connected to destination database");
            await onProgressUpdate(progress);

            // Create database if not exists
            try
            {
                await CreateDatabaseIfNotExistsAsync(destinationConnection, config.DatabaseName, _cancellationTokenSource.Token);
                progress.Logs.Add($"Ensured database {config.DatabaseName} exists");
            }
            catch (Exception ex)
            {
                progress.Logs.Add($"Error creating database: {ex.Message}");
                throw;
            }
            await onProgressUpdate(progress);

            // Create schemas if they don't exist
            var schemas = tables.Select(t => t.Schema).Distinct().ToList();
            foreach (var schema in schemas)
            {
                if (_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    progress.IsCancelled = true;
                    progress.Logs.Add("Operation cancelled by user");
                    break;
                }

                try
                {
                    await EnsureSchemaExistsAsync(destinationConnection, config.DatabaseName, schema, _cancellationTokenSource.Token);
                    progress.Logs.Add($"Ensured schema [{schema}] exists");
                }
                catch (Exception ex)
                {
                    progress.Logs.Add($"Error creating schema [{schema}]: {ex.Message}");
                    throw;
                }
            }
            await onProgressUpdate(progress);

            // Copy each table
            foreach (var table in tables)
            {
                if (_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    progress.IsCancelled = true;
                    progress.Logs.Add("Operation cancelled by user");
                    break;
                }

                progress.CurrentTableIndex++;
                progress.CurrentTable = table.FullName;
                progress.CurrentRowCount = 0; // Reset row count for new table
                progress.Logs.Add($"Copying table: {table.FullName}");
                await onProgressUpdate(progress);

                try
                {
                    await EnsureConnectionOpenAsync(sourceConnection, progress.SourceConnection, _cancellationTokenSource.Token);
                    await EnsureConnectionOpenAsync(destinationConnection, progress.DestinationConnection, _cancellationTokenSource.Token);

                    // Get row count
                    var rowCount = await GetTableRowCountAsync(sourceConnection, config.DatabaseName, table, _cancellationTokenSource.Token);
                    progress.TableRowCounts[table.FullName] = rowCount;
                    progress.Logs.Add($"Table {table.FullName} has {rowCount:N0} rows");
                    await onProgressUpdate(progress);

                    // Drop the table if we're retrying
                    if (existingProgress?.FailedTables.ContainsKey(table.FullName) == true)
                    {
                        await DropTableAsync(destinationConnection, config.DatabaseName, table, _cancellationTokenSource.Token);
                        progress.Logs.Add($"Dropped existing failed table {table.FullName} for retry");
                    }

                    // Copy schema and data
                    await CopyTableSchemaAndDataAsync(sourceConnection, destinationConnection, config.DatabaseName, table, progress, onProgressUpdate, _cancellationTokenSource.Token);
                    progress.Logs.Add($"Successfully copied table {table.FullName} ({progress.CurrentRowCount:N0} rows)");
                    
                    // Remove from failed tables if this was a retry
                    if (progress.FailedTables.ContainsKey(table.FullName))
                    {
                        progress.FailedTables.Remove(table.FullName, out _);
                    }
                }
                catch (Exception ex)
                {
                    progress.FailedTables[table.FullName] = ex.Message;
                    progress.Logs.Add($"Failed to copy table {table.FullName}: {ex.Message}");
                    // Continue with next table instead of throwing
                }

                // Update progress
                progress.ElapsedTime = stopwatch.Elapsed;
                if (progress.ElapsedTime.TotalSeconds > 0)
                {
                    progress.RowsPerSecond = progress.CurrentRowCount / progress.ElapsedTime.TotalSeconds;
                }
                await onProgressUpdate(progress);
            }

            // Set completion status
            progress.IsCompleted = !progress.IsCancelled;
            var failedCount = progress.FailedTables.Count;
            var successCount = progress.TotalTables - failedCount;
            
            if (failedCount > 0)
            {
                progress.Logs.Add($"Database copy completed with {failedCount} failed tables:");
                foreach (var failure in progress.FailedTables)
                {
                    progress.Logs.Add($"- {failure.Key}: {failure.Value}");
                }
            }
            
            if (progress.IsCancelled)
            {
                progress.Logs.Add("Operation was cancelled by user");
            }
            else
            {
                progress.Logs.Add($"Successfully copied {successCount} out of {progress.TotalTables} tables");
            }
            await onProgressUpdate(progress);
        }
        catch (Exception ex)
        {
            progress.Logs.Add($"Critical error: {ex.Message}");
            await onProgressUpdate(progress);
            throw;
        }
        finally
        {
            stopwatch.Stop();
            progress.ElapsedTime = stopwatch.Elapsed;

            if (sourceConnection != null)
            {
                UpdateConnectionStatus(sourceConnection, progress.SourceConnection);
                await sourceConnection.DisposeAsync();
            }

            if (destinationConnection != null)
            {
                UpdateConnectionStatus(destinationConnection, progress.DestinationConnection);
                await destinationConnection.DisposeAsync();
            }

            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
        }

        return progress;
    }

    private async Task<List<TableInfo>> GetTablesAsync(SqlConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(connection, new ConnectionStatus(), cancellationToken);
        var tables = new List<TableInfo>();
        var sql = $@"
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME 
            FROM [{databaseName}].INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME";

        using var command = new SqlCommand(sql, connection);
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            tables.Add(new TableInfo 
            { 
                Schema = reader.GetString(0),
                Name = reader.GetString(1)
            });
        }

        return tables;
    }

    private async Task CreateDatabaseIfNotExistsAsync(SqlConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(connection, new ConnectionStatus(), cancellationToken);
        var sql = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = @dbName)
            BEGIN
                CREATE DATABASE [{databaseName}]
            END";
        
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@dbName", databaseName);
        await command.ExecuteNonQueryAsync();
    }

    private async Task EnsureSchemaExistsAsync(SqlConnection connection, string databaseName, string schema, CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(connection, new ConnectionStatus(), cancellationToken);
        var sql = $@"
            USE [{databaseName}];
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = @schema)
            BEGIN
                EXEC('CREATE SCHEMA [' + @schema + ']')
            END";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@schema", schema);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> GetTableRowCountAsync(SqlConnection connection, string databaseName, TableInfo table, CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(connection, new ConnectionStatus(), cancellationToken);
        var sql = $"SELECT COUNT_BIG(*) FROM [{databaseName}].{table.FullName}";
        using var command = new SqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private async Task CopyTableSchemaAndDataAsync(
        SqlConnection sourceConnection,
        SqlConnection destinationConnection,
        string databaseName,
        TableInfo table,
        CopyProgress progress,
        Func<CopyProgress, Task> onProgressUpdate,
        CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(sourceConnection, progress.SourceConnection, cancellationToken);
        await EnsureConnectionOpenAsync(destinationConnection, progress.DestinationConnection, cancellationToken);
        // Get table schema details
        var sql = $@"
            USE [{databaseName}];
            SELECT 
                c.name AS ColumnName,
                t.name AS DataType,
                c.max_length AS MaxLength,
                c.precision AS Precision,
                c.scale AS Scale,
                c.is_nullable AS IsNullable,
                c.is_identity AS IsIdentity,
                OBJECT_DEFINITION(c.default_object_id) AS DefaultValue,
                cc.definition AS ComputedColumnDefinition
            FROM sys.schemas s
            INNER JOIN sys.objects o ON s.schema_id = o.schema_id
            INNER JOIN sys.columns c ON o.object_id = c.object_id
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            WHERE s.name = @schema AND o.name = @tableName
            ORDER BY c.column_id";

        using var command = new SqlCommand(sql, sourceConnection);
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@tableName", table.Name);
        
        var createTableSql = new System.Text.StringBuilder();
        createTableSql.AppendLine($"CREATE TABLE {table.FullName} (");

        using (var reader = await command.ExecuteReaderAsync())
        {
            var columns = new List<string>();
            while (await reader.ReadAsync())
            {
                var columnName = reader.GetString(0);
                var dataType = reader.GetString(1);
                var maxLength = reader.GetInt16(2);
                var precision = reader.GetByte(3);
                var scale = reader.GetByte(4);
                var isNullable = reader.GetBoolean(5);
                var isIdentity = reader.GetBoolean(6);
                var defaultValue = reader.IsDBNull(7) ? null : reader.GetString(7);
                var computedColumnDef = reader.IsDBNull(8) ? null : reader.GetString(8);

                var columnDef = new System.Text.StringBuilder($"[{columnName}] [{dataType}]");

                if (dataType.ToLower() == "varchar" || dataType.ToLower() == "nvarchar" || dataType.ToLower() == "char" || dataType.ToLower() == "nchar")
                {
                    columnDef.Append(maxLength == -1 ? "(MAX)" : $"({maxLength})");
                }
                else if (dataType.ToLower() == "decimal" || dataType.ToLower() == "numeric")
                {
                    columnDef.Append($"({precision}, {scale})");
                }

                if (isIdentity)
                {
                    columnDef.Append(" IDENTITY(1,1)");
                }

                if (computedColumnDef != null)
                {
                    columnDef.Append($" AS {computedColumnDef}");
                }
                else if (defaultValue != null)
                {
                    columnDef.Append($" DEFAULT {defaultValue}");
                }

                if (!isNullable && computedColumnDef == null)
                {
                    columnDef.Append(" NOT NULL");
                }

                columns.Add(columnDef.ToString());
            }

            createTableSql.AppendLine(string.Join("," + Environment.NewLine, columns));
            createTableSql.AppendLine(")");
        }

        // Get primary key
        sql = $@"
            SELECT 
                col.name AS ColumnName
            FROM sys.schemas s
            INNER JOIN sys.objects o ON s.schema_id = o.schema_id
            INNER JOIN sys.indexes pk ON o.object_id = pk.object_id
            INNER JOIN sys.index_columns ic ON pk.object_id = ic.object_id AND pk.index_id = ic.index_id
            INNER JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
            WHERE s.name = @schema AND o.name = @tableName AND pk.is_primary_key = 1
            ORDER BY ic.key_ordinal";

        command.CommandText = sql;
        using (var reader = await command.ExecuteReaderAsync())
        {
            var pkColumns = new List<string>();
            while (await reader.ReadAsync())
            {
                pkColumns.Add($"[{reader.GetString(0)}]");
            }

            if (pkColumns.Count > 0)
            {
                createTableSql.AppendLine($"ALTER TABLE {table.FullName} ADD CONSTRAINT [PK_{table.Schema}_{table.Name}] PRIMARY KEY CLUSTERED ({string.Join(", ", pkColumns)})");
            }
        }

        // Create table in destination
        using (var dropCommand = new SqlCommand($"USE [{databaseName}]; IF OBJECT_ID('{table.FullName}', 'U') IS NOT NULL DROP TABLE {table.FullName}", destinationConnection))
        {
            await dropCommand.ExecuteNonQueryAsync();
        }

        using (var createCommand = new SqlCommand($"USE [{databaseName}]; {createTableSql}", destinationConnection))
        {
            await createCommand.ExecuteNonQueryAsync();
        }

        // Copy data
        using (var selectCommand = new SqlCommand($"SELECT * FROM [{databaseName}].{table.FullName}", sourceConnection))
        using (var reader = await selectCommand.ExecuteReaderAsync())
        {
            using var bulkCopy = new SqlBulkCopy(destinationConnection);
            bulkCopy.DestinationTableName = $"[{databaseName}].{table.FullName}";
            bulkCopy.NotifyAfter = 1000;
            bulkCopy.BulkCopyTimeout = 0; // No timeout
            bulkCopy.SqlRowsCopied += async (sender, args) =>
            {
                progress.CurrentRowCount = args.RowsCopied;
                await onProgressUpdate(progress);
            };

            await bulkCopy.WriteToServerAsync(reader);
        }
    }

    private async Task DropTableAsync(SqlConnection connection, string databaseName, TableInfo table, CancellationToken cancellationToken)
    {
        await EnsureConnectionOpenAsync(connection, new ConnectionStatus(), cancellationToken);
        var sql = $"USE [{databaseName}]; IF OBJECT_ID('{table.FullName}', 'U') IS NOT NULL DROP TABLE {table.FullName}";
        using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
} 