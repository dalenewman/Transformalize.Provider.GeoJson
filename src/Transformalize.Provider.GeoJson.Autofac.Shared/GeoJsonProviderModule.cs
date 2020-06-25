#region license
// Transformalize
// Configurable Extract, Transform, and Load
// Copyright 2013-2020 Dale Newman
//  
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
//       http://www.apache.org/licenses/LICENSE-2.0
//   
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#endregion

using System.IO;
using System.Linq;
using Autofac;
using Newtonsoft.Json;
using Transformalize.Configuration;
using Transformalize.Containers.Autofac;
using Transformalize.Context;
using Transformalize.Contracts;
using Transformalize.Nulls;

namespace Transformalize.Providers.GeoJson.Autofac {

   /// <summary>
   /// An autofac module for everything GeoJson
   /// </summary>
   public class GeoJsonProviderModule : Module {
      
      private Process _process;
      private readonly Stream _stream;
      private const string GeoJson = "geojson";

      public IPropertyRepository PropertyRepository { get; set; } = new PropertyRepository();

      /// <summary>
      /// Create a GeoJson module with an optional stream to write to
      /// </summary>
      /// <param name="stream"></param>
      public GeoJsonProviderModule(Stream stream = null) {
         _stream = stream;
      }

      /// <summary>
      /// Create a GeoJson module with a Transformalize process and an option stream to write to
      /// </summary>
      /// <param name="process"></param>
      /// <param name="stream"></param>
      public GeoJsonProviderModule(Process process, Stream stream = null) {
         _process = process;
         _stream = stream;
      }

      /// <summary>
      /// Register everything need for GeoJson
      /// </summary>
      /// <param name="builder"></param>
      protected override void Load(ContainerBuilder builder) {

         if(_process == null && PropertyRepository != null) {
            _process = (Process) PropertyRepository.GetProperty(builder, "Process");
         }

         if (_process == null)
            return;

         // geoJson schema reading not supported yet
         foreach (var connection in _process.Connections.Where(c => c.Provider == GeoJson)) {
            builder.Register<ISchemaReader>(ctx => new NullSchemaReader()).Named<ISchemaReader>(connection.Key);
         }

         // geoJson input not supported yet
         foreach (var entity in _process.Entities.Where(e => _process.Connections.First(c => c.Name == e.Input).Provider == "geojson")) {
            // input version detector
            builder.RegisterType<NullInputProvider>().Named<IInputProvider>(entity.Key);
            // input read
            builder.Register<IRead>(ctx => {
               var input = ctx.ResolveNamed<InputContext>(entity.Key);
               return new NullReader(input, false);
            }).Named<IRead>(entity.Key);
         }

         var outputConnection = _process.GetOutputConnection();

         if (outputConnection != null) {
            if (outputConnection.Provider == GeoJson) {
               if (outputConnection.Stream) {
                  var writer = new JsonTextWriter(new StreamWriter(_stream));
                  foreach (var entity in _process.Entities) {
                     builder.Register<IWrite>(ctx => {
                        var output = ctx.ResolveNamed<OutputContext>(entity.Key);
                        return new GeoJsonMinimalProcessStreamWriter(output, writer);
                     }).Named<IWrite>(entity.Key);
                  }
               } else {
                  foreach (var entity in _process.Entities) {
                     builder.Register<IWrite>(ctx => {
                        var output = ctx.ResolveNamed<OutputContext>(entity.Key);
                        return new GeoJsonFileWriter(output);
                     }).Named<IWrite>(entity.Key);
                  }
               }
            }
         }
      }
   }
}