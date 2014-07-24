// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using JetBrains.Annotations;
using Microsoft.Data.Entity.Migrations;
using Microsoft.Data.Entity.Relational.Model;
using Microsoft.Data.Entity.SqlServer.Utilities;

namespace Microsoft.Data.Entity.SqlServer
{
    public class SqlServerMigrationOperationSqlGeneratorFactory : IMigrationOperationSqlGeneratorFactory
    {
        public virtual SqlServerMigrationOperationSqlGenerator Create([NotNull] DatabaseModel sourceDatabase, [NotNull] DatabaseModel targetDatabase)
        {
            Check.NotNull(sourceDatabase, "sourceDatabase");
            Check.NotNull(targetDatabase, "targetDatabase");

            return
                new SqlServerMigrationOperationSqlGenerator(new SqlServerTypeMapper())
                    {
                        SourceDatabase = sourceDatabase,
                        TargetDatabase = targetDatabase
                    };
        }

        MigrationOperationSqlGenerator IMigrationOperationSqlGeneratorFactory.Create(DatabaseModel sourceDatabase, DatabaseModel targetDatabase)
        {
            return Create(sourceDatabase, targetDatabase);
        }
    }
}
