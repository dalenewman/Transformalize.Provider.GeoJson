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
      private readonly JsonWriter _jsonWriter;
      private readonly Field[] _properties;

      public GeoJsonMinimalProcessStreamWriter(IContext context, JsonWriter jsonWriter) {
         _context = context;
         _jsonWriter = jsonWriter;

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

         _properties = fields.Where(f => f.Property).Except(new Field[] { _descriptionField, _colorField, _symbolField, _batchField }.Where(f => f != null)).ToArray();

      }

      public void Write(IEnumerable<IRow> rows) {

         if (Equals(_context.Process.Entities.First(), _context.Entity)) {
            _jsonWriter.WriteStartObject(); //root

            _jsonWriter.WritePropertyName("type");
            _jsonWriter.WriteValue("FeatureCollection");

            _jsonWriter.WritePropertyName("features");
            _jsonWriter.WriteStartArray();  //features
         }

         foreach (var row in rows) {

            _jsonWriter.WriteStartObject(); //feature
            _jsonWriter.WritePropertyName("type");
            _jsonWriter.WriteValue("Feature");
            _jsonWriter.WritePropertyName("geometry");
            _jsonWriter.WriteStartObject(); //geometry 
            _jsonWriter.WritePropertyName("type");
            _jsonWriter.WriteValue("Point");

            _jsonWriter.WritePropertyName("coordinates");
            _jsonWriter.WriteStartArray();
            _jsonWriter.WriteValue(row[_longitudeField]);
            _jsonWriter.WriteValue(row[_latitudeField]);
            _jsonWriter.WriteEndArray();

            _jsonWriter.WriteEndObject(); //geometry

            _jsonWriter.WritePropertyName("properties");
            _jsonWriter.WriteStartObject(); //properties

            _jsonWriter.WritePropertyName("description");
            if (_hasDescription) {
               _jsonWriter.WriteValue(row[_descriptionField]);
            } else {
               _jsonWriter.WriteValue("add geojson-description to output");
            }

            if (_hasBatchValue) {
               _jsonWriter.WritePropertyName("batch-value");
               _jsonWriter.WriteValue(row[_batchField]);
            }

            if (_hasColor) {
               _jsonWriter.WritePropertyName("marker-color");
               _jsonWriter.WriteValue(row[_colorField]);
            }

            if (_hasSymbol) {
               var symbol = row[_symbolField].ToString();
               _jsonWriter.WritePropertyName("marker-symbol");
               _jsonWriter.WriteValue(symbol);
            }

            foreach(var field in _properties) {
               var name = field.Label == string.Empty ? field.Alias : field.Label;
               _jsonWriter.WritePropertyName(name);
               _jsonWriter.WriteValue(row[field]);
            }

            _jsonWriter.WriteEndObject(); //properties

            _jsonWriter.WriteEndObject(); //feature
         }
         if (Equals(_context.Process.Entities.Last(), _context.Entity)) {
            _jsonWriter.WriteEndArray(); //features
            _jsonWriter.WriteEndObject(); //root
         }

         _jsonWriter.Flush();

      }
   }
}
