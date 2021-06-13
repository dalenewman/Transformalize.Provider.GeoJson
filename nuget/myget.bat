nuget pack Transformalize.Provider.GeoJson.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.GeoJson.Autofac.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.GeoJson.Autofac.v3.nuspec -OutputDirectory "c:\temp\modules"

nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.0.8.27-beta.nupkg" -source https://www.myget.org/F/transformalize/api/v3/index.json
nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.Autofac.0.8.27-beta.nupkg" -source https://www.myget.org/F/transformalize/api/v3/index.json
nuget push "c:\temp\modules\Transformalize.Provider.GeoJson.Autofac.v3.0.8.27-beta.nupkg" -source https://www.myget.org/F/transformalize/api/v3/index.json
