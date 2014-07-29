// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Migrations.Model;
using Microsoft.Data.Entity.Migrations.Utilities;

namespace Microsoft.Data.Entity.Migrations
{
    public class MigrationOperationCollection
    {
        private readonly Dictionary<Type, List<MigrationOperation>> _allOperations
            = new Dictionary<Type, List<MigrationOperation>>();

        public virtual void Add([NotNull] MigrationOperation operation)
        {
            Check.NotNull(operation, "operation");

            List<MigrationOperation> operations;

            if (_allOperations.TryGetValue(operation.GetType(), out operations))
            {
                operations.Add(operation);
            }
            else
            {
                _allOperations.Add(operation.GetType(), new List<MigrationOperation> { operation });
            }
        }

        public virtual void AddRange<T>([NotNull] IEnumerable<T> operations)
            where T : MigrationOperation
        {
            Check.NotNull(operations, "operations");

            List<MigrationOperation> existingOperations;

            if (_allOperations.TryGetValue(typeof(T), out existingOperations))
            {
                existingOperations.AddRange(operations);
            }
            else
            {
                _allOperations.Add(typeof(T), new List<MigrationOperation>(operations));
            }
        }

        public virtual IReadOnlyList<T> Get<T>()
            where T : MigrationOperation
        {
            List<MigrationOperation> operations;

            return
                _allOperations.TryGetValue(typeof(T), out operations)
                    ? operations.Cast<T>().ToArray()
                    : Enumerable.Empty<T>().ToArray();
        }

        public virtual void Set<T>([NotNull] IEnumerable<T> operations)
            where T : MigrationOperation
        {
            Check.NotNull(operations, "operations");

            _allOperations[typeof(T)] = new List<MigrationOperation>(operations);
        }

        public virtual IReadOnlyList<MigrationOperation> GetAll()
        {
            return
                ((IEnumerable<MigrationOperation>)Get<DropIndexOperation>())
                    .Concat(Get<DropForeignKeyOperation>())
                    .Concat(Get<DropPrimaryKeyOperation>())
                    .Concat(Get<DropColumnOperation>())
                    .Concat(Get<DropTableOperation>())
                    .Concat(Get<MoveTableOperation>())
                    .Concat(Get<RenameTableOperation>())
                    .Concat(Get<RenameColumnOperation>())
                    .Concat(Get<RenameIndexOperation>())
                    .Concat(Get<CreateTableOperation>())
                    .Concat(Get<AddColumnOperation>())
                    .Concat(Get<AlterColumnOperation>())
                    .Concat(Get<AddDefaultConstraintOperation>())
                    .Concat(Get<AddPrimaryKeyOperation>())
                    .Concat(Get<AddForeignKeyOperation>())
                    .Concat(Get<CreateIndexOperation>())
                    .ToArray();
        }
    }
}
