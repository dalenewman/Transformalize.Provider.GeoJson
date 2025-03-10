nuget pack Transformalize.Provider.GeoJson.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.GeoJson.Autofac.nuspec -OutputDirectory "c:\temp\modules"

REM nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.0.11.1-beta.nupkg" -source https://api.nuget.org/v3/index.json
REM nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.Autofac.0.11.1-beta.nupkg" -source https://api.nuget.org/v3/index.json
