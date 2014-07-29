// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Migrations;
using Microsoft.Data.Entity.Migrations.Model;
using Microsoft.Data.Entity.Relational;
using Microsoft.Data.Entity.Relational.Model;
using Microsoft.Data.Entity.SqlServer.Utilities;
using Microsoft.Data.Entity.Utilities;

namespace Microsoft.Data.Entity.SqlServer
{
    public class SqlServerMigrationOperationSqlGenerator : MigrationOperationSqlGenerator
    {
        private int _variableCount;

        public SqlServerMigrationOperationSqlGenerator([NotNull] SqlServerTypeMapper typeMapper)
            : base(typeMapper)
        {
        }

        public override IEnumerable<SqlStatement> Generate(IEnumerable<MigrationOperation> migrationOperations)
        {
            Check.NotNull(migrationOperations, "migrationOperations");

            var collection = new MigrationOperationCollection();
            foreach (var operation in migrationOperations)
            {
                collection.Add(operation);
            }

            foreach (var alterColumnOperation in collection.Get<AlterColumnOperation>())
            {
                var sourceTable = SourceDatabase.GetTable(GetSourceTableName(alterColumnOperation.TableName, migrationOperations));
                var targetTable = TargetDatabase.GetTable(GetTargetTableName(alterColumnOperation.TableName, migrationOperations));
                var sourceColumn = sourceTable.GetColumn(GetSourceColumnName(alterColumnOperation.NewColumn.Name, collection.Get<RenameColumnOperation>()));
                var targetColumn = targetTable.GetColumn(GetTargetColumnName(alterColumnOperation.NewColumn.Name, collection.Get<RenameColumnOperation>()));

                if (sourceTable.PrimaryKey.Columns.Contains(sourceColumn))
                {
                    collection.Add(new DropPrimaryKeyOperation(sourceTable.Name, sourceTable.PrimaryKey.Name));
                }

                if (targetTable.PrimaryKey.Columns.Contains(targetColumn))
                {
                    collection.Add(
                        new AddPrimaryKeyOperation(
                            targetTable.Name, 
                            targetTable.PrimaryKey.Name, 
                            targetTable.PrimaryKey.Columns.Select(c => c.Name).ToArray(), 
                            targetTable.PrimaryKey.IsClustered));
                }

                foreach (var table in SourceDatabase.Tables)
                {
                    foreach (var fk in table.ForeignKeys)
                    {
                        if (fk.ReferencedTable.Name == sourceTable.Name
                            && fk.ReferencedColumns.Contains(sourceColumn))
                        {
                            collection.Add(new DropForeignKeyOperation(table.Name, fk.Name));
                        }
                    }
                }

                foreach (var table in TargetDatabase.Tables)
                {
                    foreach (var fk in table.ForeignKeys)
                    {
                        if (fk.ReferencedTable.Name == targetTable.Name
                            && fk.ReferencedColumns.Contains(targetColumn))
                        {
                            collection.Add(
                                new AddForeignKeyOperation(
                                    fk.Table.Name, 
                                    fk.Name, 
                                    fk.Columns.Select(c => c.Name).ToArray(), 
                                    fk.ReferencedTable.Name, 
                                    fk.ReferencedColumns.Select(c => c.Name).ToArray(),
                                    fk.CascadeDelete));
                        }
                    }
                }

                if (sourceColumn.HasDefault)
                {
                    collection.Add(new DropDefaultConstraintOperation(sourceTable.Name, sourceColumn.Name));
                }
            }

            return base.Generate(collection.GetAll());
        }

        private SchemaQualifiedName GetSourceTableName(SchemaQualifiedName tableName, IEnumerable<MigrationOperation> operations)
        {
            foreach (var operation in operations.Reverse())
            {
                var moveTableOperation = operation as MoveTableOperation;
                if (moveTableOperation != null)
                {
                    if (tableName.Schema == moveTableOperation.NewSchema)
                    {
                        tableName = new SchemaQualifiedName(tableName.Name, moveTableOperation.TableName.Schema);
                    }

                    continue;
                }

                var renameTableOperation = operation as RenameTableOperation;
                if (renameTableOperation != null)
                {
                    if (tableName.Name == renameTableOperation.NewTableName)
                    {
                        tableName = new SchemaQualifiedName(renameTableOperation.TableName.Name, tableName.Schema);
                    }
                }
            }

            return tableName;
        }

        private SchemaQualifiedName GetTargetTableName(SchemaQualifiedName tableName, IEnumerable<MigrationOperation> operations)
        {
            foreach (var operation in operations)
            {
                var moveTableOperation = operation as MoveTableOperation;
                if (moveTableOperation != null)
                {
                    if (tableName.Schema == moveTableOperation.TableName.Schema)
                    {
                        tableName = new SchemaQualifiedName(tableName.Name, moveTableOperation.NewSchema);
                    }

                    continue;
                }

                var renameTableOperation = operation as RenameTableOperation;
                if (renameTableOperation != null)
                {
                    if (tableName.Name == renameTableOperation.TableName.Name)
                    {
                        tableName = new SchemaQualifiedName(renameTableOperation.NewTableName, tableName.Schema);
                    }
                }
            }

            return tableName;
        }

        private string GetSourceColumnName(string columnName, IEnumerable<RenameColumnOperation> operations)
        {
            foreach (var operation in operations.Reverse())
            {
                if (columnName == operation.NewColumnName)
                {
                    columnName = operation.ColumnName;
                }
            }

            return columnName;
        }

        private string GetTargetColumnName(string columnName, IEnumerable<RenameColumnOperation> operations)
        {
            foreach (var operation in operations)
            {
                if (columnName == operation.ColumnName)
                {
                    columnName = operation.ColumnName;
                }
            }

            return columnName;
        }

        public override void Generate(RenameTableOperation renameTableOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameTableOperation, "renameTableOperation");

            stringBuilder
                .Append("EXECUTE sp_rename @objname = N")
                .Append(DelimitLiteral(renameTableOperation.TableName))
                .Append(", @newname = N")
                .Append(DelimitLiteral(renameTableOperation.NewTableName))
                .Append(", @objtype = N'OBJECT'");
        }

        public override void Generate(AddDefaultConstraintOperation addDefaultConstraintOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(addDefaultConstraintOperation, "addDefaultConstraintOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            var tableName = addDefaultConstraintOperation.TableName;
            var columnName = addDefaultConstraintOperation.ColumnName;

            stringBuilder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(tableName))
                .Append(" ADD CONSTRAINT ")
                .Append(DelimitIdentifier("DF_" + tableName + "_" + columnName))
                .Append(" DEFAULT ");

            stringBuilder.Append(addDefaultConstraintOperation.DefaultSql ?? GenerateLiteral(addDefaultConstraintOperation.DefaultValue));

            stringBuilder
                .Append(" FOR ")
                .Append(DelimitIdentifier(columnName));
        }

        public override void Generate(DropDefaultConstraintOperation dropDefaultConstraintOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropDefaultConstraintOperation, "dropDefaultConstraintOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            var constraintNameVariable = "@var" + _variableCount++;

            stringBuilder
                .Append("DECLARE ")
                .Append(constraintNameVariable)
                .AppendLine(" nvarchar(128)");

            stringBuilder
                .Append("SELECT ")
                .Append(constraintNameVariable)
                .Append(" = name FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N")
                .Append(DelimitLiteral(dropDefaultConstraintOperation.TableName))
                .Append(") AND COL_NAME(parent_object_id, parent_column_id) = N")
                .AppendLine(DelimitLiteral(dropDefaultConstraintOperation.ColumnName));

            stringBuilder
                .Append("EXECUTE('ALTER TABLE ")
                .Append(DelimitIdentifier(dropDefaultConstraintOperation.TableName))
                .Append(" DROP CONSTRAINT \"' + ")
                .Append(constraintNameVariable)
                .Append(" + '\"')");
        }

        public override void Generate(RenameColumnOperation renameColumnOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameColumnOperation, "renameColumnOperation");

            stringBuilder
                .Append("EXECUTE sp_rename @objname = N'")
                .Append(EscapeLiteral(renameColumnOperation.TableName))
                .Append(".")
                .Append(EscapeLiteral(renameColumnOperation.ColumnName))
                .Append("', @newname = N")
                .Append(DelimitLiteral(renameColumnOperation.NewColumnName))
                .Append(", @objtype = N'COLUMN'");
        }

        public override void Generate(RenameIndexOperation renameIndexOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameIndexOperation, "renameIndexOperation");

            stringBuilder
                .Append("EXECUTE sp_rename @objname = N'")
                .Append(EscapeLiteral(renameIndexOperation.TableName))
                .Append(".")
                .Append(EscapeLiteral(renameIndexOperation.IndexName))
                .Append("', @newname = N")
                .Append(DelimitLiteral(renameIndexOperation.NewIndexName))
                .Append(", @objtype = N'INDEX'");
        }

        public override string DelimitIdentifier(string identifier)
        {
            Check.NotEmpty(identifier, "identifier");

            return "[" + EscapeIdentifier(identifier) + "]";
        }

        public override string EscapeIdentifier(string identifier)
        {
            Check.NotEmpty(identifier, "identifier");

            return identifier.Replace("]", "]]");
        }

        protected override void GenerateColumnTraits(Column column, IndentedStringBuilder stringBuilder)
        {
            if (column.ValueGenerationStrategy == ValueGenerationOnSave.WhenInserting)
            {
                stringBuilder.Append(" IDENTITY");
            }
        }

        protected override void GeneratePrimaryKeyTraits(
            AddPrimaryKeyOperation primaryKeyOperation,
            IndentedStringBuilder stringBuilder)
        {
            if (!primaryKeyOperation.IsClustered)
            {
                stringBuilder.Append(" NONCLUSTERED");
            }
        }

        public override void Generate(DropIndexOperation dropIndexOperation, IndentedStringBuilder stringBuilder)
        {
            base.Generate(dropIndexOperation, stringBuilder);

            stringBuilder
                .Append(" ON ")
                .Append(DelimitIdentifier(dropIndexOperation.TableName));
        }
    }
}
