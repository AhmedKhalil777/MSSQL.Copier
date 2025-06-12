using System.Text.Json;
using MSSQL.Copier.Server.Models;

namespace MSSQL.Copier.Server.Services;

public class ConnectionProfileService
{
    private readonly string _configPath;
    private Dictionary<string, ConnectionProfile> _profiles;

    public ConnectionProfileService(IWebHostEnvironment env)
    {
        _configPath = Path.Combine(env.ContentRootPath, "connectionProfiles.json");
        _profiles = LoadProfiles();
    }

    public IEnumerable<ConnectionProfile> GetProfiles()
    {
        return _profiles.Values.OrderByDescending(p => p.LastUsed);
    }

    public ConnectionProfile? GetProfile(string name)
    {
        return _profiles.TryGetValue(name, out var profile) ? profile : null;
    }

    public void SaveProfile(ConnectionProfile profile)
    {
        _profiles[profile.Name] = profile;
        SaveProfiles();
    }

    public void DeleteProfile(string name)
    {
        if (_profiles.Remove(name))
        {
            SaveProfiles();
        }
    }

    public void UpdateProfileStats(string name, CopyProgress progress)
    {
        if (_profiles.TryGetValue(name, out var profile))
        {
            profile.UpdateStats(progress);
            SaveProfiles();
        }
    }

    private Dictionary<string, ConnectionProfile> LoadProfiles()
    {
        try
        {
            if (File.Exists(_configPath))
            {
                var json = File.ReadAllText(_configPath);
                return JsonSerializer.Deserialize<Dictionary<string, ConnectionProfile>>(json) 
                    ?? new Dictionary<string, ConnectionProfile>();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading profiles: {ex.Message}");
        }
        return new Dictionary<string, ConnectionProfile>();
    }

    private void SaveProfiles()
    {
        try
        {
            var json = JsonSerializer.Serialize(_profiles, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            File.WriteAllText(_configPath, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error saving profiles: {ex.Message}");
        }
    }
} 