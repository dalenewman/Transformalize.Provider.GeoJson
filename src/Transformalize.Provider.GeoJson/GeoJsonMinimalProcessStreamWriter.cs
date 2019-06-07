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
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using Transformalize.Configuration;
using Transformalize.Contracts;
using System.IO;

namespace Transformalize.Providers.GeoJson {

   public class GeoJsonMinimalProcessStreamWriter : IWrite {

      private readonly Field _latitudeField;
      private readonly Field _longitudeField;
      private readonly Field _colorField;
      private readonly Field _symbolField;
      private readonly Field _descriptionField;
      private readonly Field _batchField;
      private readonly bool _hasColor;
      private readonly bool _hasSymbol;
      private readonly bool _hasDescription;
      private readonly bool _hasBatchValue;
      private readonly IContext _context;
      private readonly Stream _stream;

      public GeoJsonMinimalProcessStreamWriter(IContext context, Stream stream) {
         _context = context;
         _stream = stream;

         var fields = context.GetAllEntityFields().ToArray();

         _latitudeField = fields.FirstOrDefault(f => f.Alias.ToLower() == "latitude") ?? fields.FirstOrDefault(f => f.Alias.ToLower().StartsWith("lat"));
         _longitudeField = fields.FirstOrDefault(f => f.Alias.ToLower() == "longitude") ?? fields.FirstOrDefault(f => f.Alias.ToLower().StartsWith("lon"));
         _colorField = fields.FirstOrDefault(f => f.Alias.ToLower() == "geojson-color");
         _symbolField = fields.FirstOrDefault(f => f.Alias.ToLower() == "geojson-symbol");

         _descriptionField = fields.FirstOrDefault(f => f.Alias.ToLower() == "geojson-description");
         _batchField = fields.FirstOrDefault(f => f.Alias.ToLower() == "batchvalue");

         _hasColor = _colorField != null;
         _hasSymbol = _symbolField != null;
         _hasDescription = _descriptionField != null;
         _hasBatchValue = _batchField != null;

      }

      public void Write(IEnumerable<IRow> rows) {

         var textWriter = new StreamWriter(_stream);
         var writer = new JsonTextWriter(textWriter);

         if (Equals(_context.Process.Entities.First(), _context.Entity)) {
            writer.WriteStartObject(); //root

            writer.WritePropertyName("type");
            writer.WriteValue("FeatureCollection");

            writer.WritePropertyName("features");
            writer.WriteStartArray();  //features
         }

         foreach (var row in rows) {

            writer.WriteStartObject(); //feature
            writer.WritePropertyName("type");
            writer.WriteValue("Feature");
            writer.WritePropertyName("geometry");
            writer.WriteStartObject(); //geometry 
            writer.WritePropertyName("type");
            writer.WriteValue("Point");

            writer.WritePropertyName("coordinates");
            writer.WriteStartArray();
            writer.WriteValue(row[_longitudeField]);
            writer.WriteValue(row[_latitudeField]);
            writer.WriteEndArray();

            writer.WriteEndObject(); //geometry

            writer.WritePropertyName("properties");
            writer.WriteStartObject(); //properties

            writer.WritePropertyName("description");
            if (_hasDescription) {
               writer.WriteValue(row[_descriptionField]);
            } else {
               writer.WriteValue("add geojson-description to output");
            }

            if (_hasBatchValue) {
               writer.WritePropertyName("batch-value");
               writer.WriteValue(row[_batchField]);
            }

            if (_hasColor) {
               writer.WritePropertyName("marker-color");
               writer.WriteValue(row[_colorField]);
            }

            if (_hasSymbol) {
               var symbol = row[_symbolField].ToString();
               writer.WritePropertyName("marker-symbol");
               writer.WriteValue(symbol);
            }

            writer.WriteEndObject(); //properties

            writer.WriteEndObject(); //feature
         }
         if (Equals(_context.Process.Entities.Last(), _context.Entity)) {
            writer.WriteEndArray(); //features
            writer.WriteEndObject(); //root
         }

         writer.Flush();
      }
   }
}
