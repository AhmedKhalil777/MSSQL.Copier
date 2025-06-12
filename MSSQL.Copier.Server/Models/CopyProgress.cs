using System.Collections.Concurrent;

namespace MSSQL.Copier.Server.Models;

public class ConnectionStatus
{
    public bool IsConnected { get; set; }
    public string State { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
    public string Server { get; set; } = string.Empty;
}

public class CopyProgress
{
    public bool IsCompleted { get; set; }
    public bool IsCancelled { get; set; }
    public bool IsRetrying { get; set; }
    public int RetryAttempt { get; set; }
    public HashSet<string> RetriedTables { get; set; } = new HashSet<string>();
    
    // Parallel processing properties
    public HashSet<string> ActiveTables { get; set; } = new();
    public HashSet<string> CompletedTables { get; set; } = new();
    public ConcurrentDictionary<string, long> TableProgress { get; set; } = new();
    
    // Legacy properties - keeping for backward compatibility
    public string CurrentTable { get; set; } = string.Empty;
    public int CurrentTableIndex { get; set; }
    public int TotalTables { get; set; }
    public long CurrentRowCount { get; set; }
    public double CurrentTableProgress { get; set; }
    public double OverallProgress { get; set; }
    
    public TimeSpan ElapsedTime { get; set; }
    public double RowsPerSecond { get; set; }
    public Dictionary<string, long> TableRowCounts { get; set; } = new Dictionary<string, long>();
    public Dictionary<string, string> FailedTables { get; set; } = new Dictionary<string, string>();
    public List<string> Logs { get; set; } = new List<string>();
    public ConnectionStatus SourceConnection { get; set; } = new ConnectionStatus();
    public ConnectionStatus DestinationConnection { get; set; } = new ConnectionStatus();

    // Helper method to calculate overall progress
    public double CalculateOverallProgress()
    {
        if (TotalTables == 0) return 0;
        return Math.Min((CompletedTables.Count * 100.0) / TotalTables, 100);
    }

    // Helper method to calculate table progress
    public double CalculateTableProgress(string tableName)
    {
        if (!TableRowCounts.TryGetValue(tableName, out var total) || total == 0)
            return 0;

        if (!TableProgress.TryGetValue(tableName, out var current))
            return 0;

        return Math.Min((current * 100.0) / total, 100);
    }

    // Helper method to update table progress
    public void UpdateTableProgress(string tableName, long rowsCopied)
    {
        TableProgress.AddOrUpdate(tableName, rowsCopied, (_, _) => rowsCopied);
        
        // Update legacy properties for backward compatibility
        if (tableName == CurrentTable)
        {
            CurrentRowCount = rowsCopied;
            if (TableRowCounts.TryGetValue(tableName, out var total) && total > 0)
            {
                CurrentTableProgress = Math.Min((rowsCopied * 100.0) / total, 100);
            }
        }
    }

    // Helper method to mark table as completed
    public void MarkTableCompleted(string tableName)
    {
        ActiveTables.Remove(tableName);
        CompletedTables.Add(tableName);
        OverallProgress = CalculateOverallProgress();
    }

    // Helper method to start table processing
    public void StartTableProcessing(string tableName)
    {
        ActiveTables.Add(tableName);
        TableProgress.TryAdd(tableName, 0);
        
        // Update legacy property
        CurrentTable = tableName;
    }
} 