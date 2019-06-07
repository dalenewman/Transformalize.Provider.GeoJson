#region license
// Transformalize
// Configurable Extract, Transform, and Load
// Copyright 2013-2019 Dale Newman
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

using System.Linq;
using Autofac;
using Transformalize.Configuration;
using Transformalize.Context;
using Transformalize.Contracts;
using Transformalize.Nulls;

namespace Transformalize.Providers.GeoJson.Autofac {
   public class GeoJsonModule : Module {

      public GeoJsonModule() { }

      protected override void Load(ContainerBuilder builder) {

         if (!builder.Properties.ContainsKey("Process")) {
            return;
         }

         var process = (Process)builder.Properties["Process"];

         // geoJson schema reading not supported yet
         foreach (var connection in process.Connections.Where(c => c.Provider == "geojson")) {
            builder.Register<ISchemaReader>(ctx => new NullSchemaReader()).Named<ISchemaReader>(connection.Key);
         }

         // geoJson input not supported yet
         foreach (var entity in process.Entities.Where(e => process.Connections.First(c => c.Name == e.Connection).Provider == "geojson")) {

            // input version detector
            builder.RegisterType<NullInputProvider>().Named<IInputProvider>(entity.Key);

            // input read
            builder.Register<IRead>(ctx => {
               var input = ctx.ResolveNamed<InputContext>(entity.Key);
               return new NullReader(input, false);
            }).Named<IRead>(entity.Key);

         }

         if (process.Output().Provider == "geojson") {

            foreach (var entity in process.Entities) {

               // ENTITY WRITER
               builder.Register<IWrite>(ctx => {
                  return new GeoJsonFileWriter(ctx.ResolveNamed<OutputContext>(entity.Key));
               }).Named<IWrite>(entity.Key);
            }
         }
      }
   }
}