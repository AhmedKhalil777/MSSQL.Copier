using System.ComponentModel.DataAnnotations;

namespace MSSQL.Copier.Server.Models;

public class DatabaseConfig
{
    private string _connectionString = string.Empty;
    private string _destinationConnectionString = string.Empty;

    [Required(ErrorMessage = "Source connection string is required")]
    public string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = EnsureTrustServerCertificate(value);
    }

    [Required(ErrorMessage = "Database name is required")]
    public string DatabaseName { get; set; } = string.Empty;

    public bool IsLocalDestination { get; set; }

    [Required(ErrorMessage = "Destination connection string is required when not copying locally")]
    public string DestinationConnectionString
    {
        get => _destinationConnectionString;
        set => _destinationConnectionString = EnsureTrustServerCertificate(value);
    }

    [Range(1, 10, ErrorMessage = "Parallel tables must be between 1 and 10")]
    public int MaxParallelTables { get; set; } = 3;

    private string EnsureTrustServerCertificate(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return connectionString;
        
        if (!connectionString.Contains("TrustServerCertificate=", StringComparison.OrdinalIgnoreCase))
        {
            return connectionString.TrimEnd(';') + ";TrustServerCertificate=True";
        }
        
        return connectionString;
    }
} 