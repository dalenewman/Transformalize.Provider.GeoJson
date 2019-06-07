nuget pack Transformalize.Provider.GeoJson.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.GeoJson.Autofac.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.GeoJson.Autofac.v3.nuspec -OutputDirectory "c:\temp\modules"

REM nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.0.6.9-beta.nupkg" -source https://api.nuget.org/v3/index.json
REM nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.Autofac.0.6.9-beta.nupkg" -source https://api.nuget.org/v3/index.json
REM nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.Autofac.v3.0.6.9-beta.nupkg" -source https://api.nuget.org/v3/index.json
