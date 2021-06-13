#region license
// Transformalize
// Configurable Extract, Transform, and Load
// Copyright 2013-2021 Dale Newman
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
using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using Transformalize.Configuration;
using Transformalize.Contracts;

namespace Transformalize.Providers.GeoJson {

   /// <summary>
   /// Write a process' output as GeoJson to a stream with an emphasis on a light payload
   /// </summary>
   public class GeoJsonMinimalProcessStreamWriter2 : IWrite {

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
      private readonly Utf8JsonWriter _jw;
      private readonly Field[] _properties;

      /// <summary>
      /// Given a context and a JSON Writer, prepare to write
      /// </summary>
      /// <param name="context"></param>
      /// <param name="jsonWriter"></param>
      public GeoJsonMinimalProcessStreamWriter2(IContext context, Utf8JsonWriter jsonWriter) {
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
            _jw.WriteStartObject(); //root

            _jw.WriteString("type", "FeatureCollection");

            _jw.WritePropertyName("features");
            _jw.WriteStartArray();  //features
         }

         foreach (var row in rows) {

            _jw.WriteStartObject(); //feature
            _jw.WriteString("type", "Feature");

            _jw.WritePropertyName("geometry");
            _jw.WriteStartObject(); //geometry 
            _jw.WriteString("type", "Point");

            _jw.WritePropertyName("coordinates");
            _jw.WriteStartArray();
            _jw.WriteNumberValue(System.Convert.ToDouble(row[_longitudeField]));
            _jw.WriteNumberValue(System.Convert.ToDouble(row[_latitudeField]));
            _jw.WriteEndArray();

            _jw.WriteEndObject(); //geometry

            _jw.WritePropertyName("properties");
            _jw.WriteStartObject(); //properties

            if (_hasDescription) {
               _jw.WriteString("description", row[_descriptionField].ToString());
            }

            if (_hasBatchValue) {
               WriteField(_jw, _batchField, row, "batch-value");
            }

            if (_hasColor) {
               _jw.WriteString("marker-color", row[_colorField].ToString());
            }

            if (_hasSymbol) {
               _jw.WriteString("marker-symbol", row[_symbolField].ToString());
            }

            foreach (var field in _properties) {
               WriteField(_jw, field, row);
            }

            _jw.WriteEndObject(); //properties

            _jw.WriteEndObject(); //feature

            _context.Entity.Inserts++;

            if (_context.Entity.Inserts % 50 == 0) {
               _jw.FlushAsync();
            }
         }
         if (Equals(_context.Process.Entities.Last(), _context.Entity)) {
            _jw.WriteEndArray(); //features
            _jw.WriteEndObject(); //root
         }

         _jw.FlushAsync();

      }

      private static void WriteField(Utf8JsonWriter writer, Field field, IRow row, string alias = null) {
         var name = alias ?? (field.Label == string.Empty ? field.Alias : field.Label);
         switch (field.Type) {
            case "bool":
            case "boolean":
               writer.WriteBoolean(name, (bool)row[field]);
               break;
            case "byte":
               writer.WriteNumber(name, System.Convert.ToUInt32(row[field]));
               break;
            case "byte[]":
               writer.WriteBase64String(name, (byte[])row[field]);
               break;
            case "date":
            case "datetime":
               writer.WriteString(name, (System.DateTime)row[field]);
               break;
            case "decimal":
               writer.WriteNumber(name, (decimal)row[field]);
               break;
            case "double":
               writer.WriteNumber(name, (double)row[field]);
               break;
            case "float":
            case "real":
            case "single":
               writer.WriteNumber(name, (float)row[field]);
               break;
            case "guid":
               writer.WriteString(name, (System.Guid)row[field]);
               break;
            case "int":
            case "int32":
               writer.WriteNumber(name, (int)row[field]);
               break;
            case "int64":
            case "long":
               writer.WriteNumber(name, (long)row[field]);
               break;
            case "int16":
            case "short":
               writer.WriteNumber(name, (short)row[field]);
               break;
            case "string":
               writer.WriteString(name, (string)row[field]);
               break;
            case "uint":
            case "uint32":
               writer.WriteNumber(name, (uint)row[field]);
               break;
            case "uint16":
            case "ushort":
               writer.WriteNumber(name, (ushort)row[field]);
               break;
            case "uint64":
            case "ulong":
               writer.WriteNumber(name, (ulong)row[field]);
               break;
            default:  // char, object
               writer.WriteString(name, row[field].ToString());
               break;
         }

      }
   }
}
