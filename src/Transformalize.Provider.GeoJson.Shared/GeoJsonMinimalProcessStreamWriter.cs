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

   /// <summary>
   /// Write a process' output as GeoJson to a stream with an emphasis on a light payload
   /// </summary>
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
      private readonly JsonWriter _jw;
      private readonly Field[] _properties;

      /// <summary>
      /// Given a context and a JSON Writer, prepare to write
      /// </summary>
      /// <param name="context"></param>
      /// <param name="jsonWriter"></param>
      public GeoJsonMinimalProcessStreamWriter(IContext context, JsonWriter jsonWriter) {
         _context = context;
         _jw = jsonWriter;

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

      /// <summary>
      /// Write transformalize rows as GeoJson to a stream
      /// </summary>
      /// <param name="rows">transformalize rows</param>
      public void Write(IEnumerable<IRow> rows) {

         if (Equals(_context.Process.Entities.First(), _context.Entity)) {
            _jw.WriteStartObjectAsync(); //root

            _jw.WritePropertyNameAsync("type");
            _jw.WriteValueAsync("FeatureCollection");

            _jw.WritePropertyNameAsync("features");
            _jw.WriteStartArrayAsync();  //features
         }

         foreach (var row in rows) {

            _jw.WriteStartObjectAsync(); //feature
            _jw.WritePropertyNameAsync("type");
            _jw.WriteValueAsync("Feature");
            _jw.WritePropertyNameAsync("geometry");
            _jw.WriteStartObjectAsync(); //geometry 
            _jw.WritePropertyNameAsync("type");
            _jw.WriteValueAsync("Point");

            _jw.WritePropertyNameAsync("coordinates");
            _jw.WriteStartArrayAsync();
            _jw.WriteValueAsync(row[_longitudeField]);
            _jw.WriteValueAsync(row[_latitudeField]);
            _jw.WriteEndArrayAsync();

            _jw.WriteEndObjectAsync(); //geometry

            _jw.WritePropertyNameAsync("properties");
            _jw.WriteStartObjectAsync(); //properties

            if (_hasDescription) {
               _jw.WritePropertyNameAsync("description");
               _jw.WriteValueAsync(row[_descriptionField]);
            }

            if (_hasBatchValue) {
               _jw.WritePropertyNameAsync("batch-value");
               _jw.WriteValueAsync(row[_batchField]);
            }

            if (_hasColor) {
               _jw.WritePropertyNameAsync("marker-color");
               _jw.WriteValueAsync(row[_colorField]);
            }

            if (_hasSymbol) {
               var symbol = row[_symbolField].ToString();
               _jw.WritePropertyNameAsync("marker-symbol");
               _jw.WriteValueAsync(symbol);
            }

            foreach (var field in _properties) {
               var name = field.Label == string.Empty ? field.Alias : field.Label;
               _jw.WritePropertyNameAsync(name);
               _jw.WriteValueAsync(row[field]);
            }

            _jw.WriteEndObjectAsync(); //properties

            _jw.WriteEndObjectAsync(); //feature
         }
         if (Equals(_context.Process.Entities.Last(), _context.Entity)) {
            _jw.WriteEndArrayAsync(); //features
            _jw.WriteEndObjectAsync(); //root
         }

         _jw.FlushAsync();

      }
   }
}
