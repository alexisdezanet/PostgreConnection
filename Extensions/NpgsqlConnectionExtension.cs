using Npgsql;
using PostgreConnection.Attributes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace PostgreConnection.Extensions
{
    public static class NpgsqlConnectionExtension
    {
        public static void BulkInsert<T>(this IDbConnection conn, IEnumerable<T> data)
        {
            using var connection = conn as NpgsqlConnection;

            if (connection == null)
                return;

            if (connection.State != ConnectionState.Open)
                connection.Open();

            data ??= new List<T>();

            var type = typeof(T);

            if (!TypeCache.TryGetValue(type, out var typeInfo))
            {
                typeInfo = new TypeInfo(type);
                TypeCache[type] = typeInfo;
            }

            using var transaction = connection.BeginTransaction();

            try
            {
                BulkCopyDataToTable(connection, data, typeInfo);

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
            finally
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        public static void BulkSafeInsert<T>(this IDbConnection conn, IEnumerable<T> data)
        {
            using var connection = conn as NpgsqlConnection;

            if (connection == null)
                return;

            if (connection.State != ConnectionState.Open)
                connection.Open();

            data ??= new List<T>();

            var type = typeof(T);

            if (!TypeCache.TryGetValue(type, out var typeInfo))
            {
                typeInfo = new TypeInfo(type);
                TypeCache[type] = typeInfo;
            }

            using var transaction = connection.BeginTransaction();

            var tempTableName = $"tmp_{typeInfo.TableName}_{Guid.NewGuid():N}";
            CreateTemporaryTable(connection, typeInfo, tempTableName);
            try
            {
                BulkCopyDataToTable(connection, data, typeInfo, tempTableName);
                InsertDistinctDataFromTemporaryTable(connection, typeInfo, tempTableName);

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                connection.BulkSafeInsert(data);
                throw;
            }
            finally
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        private static void CreateTemporaryTable(NpgsqlConnection connection, TypeInfo typeInfo, string tableName)
        {
            using var createTableCommand = connection.CreateCommand();
            createTableCommand.CommandText = $"CREATE TEMP TABLE {tableName} ON COMMIT DROP AS SELECT {typeInfo.ColumnsString} FROM {typeInfo.TableName} WITH NO DATA";
            createTableCommand.ExecuteNonQuery();
        }

        private static void BulkCopyDataToTable<T>(NpgsqlConnection connection, IEnumerable<T> data, TypeInfo typeInfo, string? tableName = null)
        {
            tableName ??= typeInfo.TableName;

            using var writer = connection.BeginBinaryImport($"COPY {tableName} ({typeInfo.ColumnsString}) FROM STDIN (FORMAT BINARY)");
            foreach (var item in data)
            {
                WriteDataToBinaryWriter(writer, item, typeInfo);
            }

            writer.Complete();
        }

        private static void InsertDistinctDataFromTemporaryTable(NpgsqlConnection connection, TypeInfo typeInfo, string tempTableName)
        {
            using var insertCommand = connection.CreateCommand();
            insertCommand.CommandText = $@"
                INSERT INTO {typeInfo.TableName} ({typeInfo.ColumnsString})
                SELECT DISTINCT ON ({typeInfo.DistinctString}) {typeInfo.ColumnsString}
                FROM {tempTableName} tmp
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {typeInfo.TableName} main 
                    WHERE {typeInfo.Distinct.Select(c => $"main.{c} = tmp.{c}").Aggregate((c1, c2) => $"{c1} AND {c2}")}
                )";
            insertCommand.ExecuteNonQuery();
        }

        private static void WriteDataToBinaryWriter<T>(NpgsqlBinaryImporter writer, T item, TypeInfo typeInfo)
        {
            writer.StartRow();

            foreach (var property in typeInfo.Properties)
            {
                if (property.GetCustomAttribute<IgnorePropertyAttribute>() != null)
                    continue;

                var value = property.GetValue(item, null);

                if (Nullable.GetUnderlyingType(property.PropertyType) != null && value == null)
                {
                    writer.WriteNull();
                    continue;
                }

                if (value is DateTime && (DateTime)value == DateTime.MinValue)
                    value = null;

                writer.Write(value);
            }
        }

        private static readonly Dictionary<Type, TypeInfo> TypeCache = new Dictionary<Type, TypeInfo>();

        private class TypeInfo
        {
            public string TableName { get; }
            public string[] Columns { get; }
            public string ColumnsString => string.Join(",", Columns);
            public PropertyInfo[] Properties { get; }
            public string[] Distinct { get; }
            public string DistinctString => string.Join(",", Distinct);

            public TypeInfo(Type type)
            {
                TableName = type.GetCustomAttribute<TableNameAttribute>()?.Name ?? type.Name;

                Properties = type.GetProperties();

                Distinct = Properties.Where(property => property.GetCustomAttribute<DistinctPropertyAttribute>() != null).Select(p => p.Name).ToArray();

                Columns = Properties
                    .Where(property => property.GetCustomAttribute<IgnorePropertyAttribute>() == null)
                    .Select(property => property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name)
                    .ToArray();
            }
        }
    }
}
