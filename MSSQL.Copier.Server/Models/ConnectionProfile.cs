using System.Text.Json.Serialization;

namespace MSSQL.Copier.Server.Models;

public class ConnectionProfile
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string SourceConnectionString { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public bool IsLocalDestination { get; set; }
    public string DestinationConnectionString { get; set; } = string.Empty;
    public DateTime LastUsed { get; set; }
    public long TotalRowsCopied { get; set; }
    public int TotalTablesCopied { get; set; }
    public TimeSpan TotalCopyTime { get; set; }
    public double AverageRowsPerSecond { get; set; }
    public int MaxParallelTables { get; set; } = 3; // Default to 3 parallel tables

    public DatabaseConfig ToDatabaseConfig()
    {
        return new DatabaseConfig
        {
            ConnectionString = SourceConnectionString,
            DatabaseName = DatabaseName,
            IsLocalDestination = IsLocalDestination,
            DestinationConnectionString = DestinationConnectionString,
            MaxParallelTables = MaxParallelTables
        };
    }

    public void UpdateStats(CopyProgress progress)
    {
        LastUsed = DateTime.UtcNow;
        TotalRowsCopied += progress.CurrentRowCount;
        TotalTablesCopied += progress.CurrentTableIndex;
        TotalCopyTime += progress.ElapsedTime;
        AverageRowsPerSecond = progress.RowsPerSecond;
    }
} 