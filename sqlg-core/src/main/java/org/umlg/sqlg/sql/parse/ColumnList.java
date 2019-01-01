package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.Topology;

import java.util.*;
import java.util.stream.Collectors;

/**
 * List of column, managing serialization to SQL
 *
 * @author jpmoresmau
 * @author pieter
 */
public class ColumnList {
    /**
     * Column -> alias
     */
    private final LinkedHashMap<Column, String> columns = new LinkedHashMap<>();
    /**
     * Alias -> Column
     */
    private final LinkedHashMap<String, Column> aliases = new LinkedHashMap<>();

    /**
     * Indicates that the query is for a {@link org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep}
     * In this case only the first column will be returned.
     */
    private final boolean drop;

    /**
     * the graph to have access to the SQL dialect
     */
    private final SqlgGraph sqlgGraph;

    /**
     * A map of all the properties and their types.
     */
    private final Map<String, Map<String, PropertyType>> filteredAllTables;

    /**
     * Indicates if any of the Column's have an aggregateFunction
     */
    private boolean containsAggregate;

    /**
     * build a new empty column list
     *
     * @param graph
     * @param drop
     * @param filteredAllTables
     */
    ColumnList(SqlgGraph graph, boolean drop, Map<String, Map<String, PropertyType>> filteredAllTables) {
        super();
        this.sqlgGraph = graph;
        this.drop = drop;
        this.filteredAllTables = filteredAllTables;
    }

    /**
     * add a new column
     *
     * @param schema
     * @param table
     * @param column
     * @param stepDepth
     * @param alias
     */
    private Column add(String schema, String table, String column, int stepDepth, String alias, String aggregateFunction) {
        Column c = new Column(schema, table, column, this.filteredAllTables.get(schema + "." + table).get(column), stepDepth, aggregateFunction);
        this.columns.put(c, alias);
        this.aliases.put(alias, c);
        this.containsAggregate = this.containsAggregate || aggregateFunction != null;
        return c;
    }

    private Column add(String schema, String table, String column, int stepDepth, String alias) {
        return add(schema, table, column, stepDepth, alias, null);
    }

    public void add(SchemaTableTree stt, String column, String alias) {
        add(stt.getSchemaTable(), column, stt.getStepDepth(), alias, stt.getAggregateFunction() == null ? null : stt.getAggregateFunction().getLeft());
    }

    public void add(SchemaTableTree stt, String column, String alias, String aggregateFunction) {
        add(stt.getSchemaTable(), column, stt.getStepDepth(), alias, aggregateFunction);
    }

    public void add(SchemaTable st, String column, int stepDepth, String alias) {
        add(st.getSchema(), st.getTable(), column, stepDepth, alias);
    }

    public void add(SchemaTable st, String column, int stepDepth, String alias, String aggregateFunction) {
        add(st.getSchema(), st.getTable(), column, stepDepth, alias, aggregateFunction);
    }

    boolean isContainsAggregate() {
        return containsAggregate;
    }

    /**
     * add a new column
     *
     * @param schema          The column's schema
     * @param table           The column's table
     * @param column          The column
     * @param stepDepth       The column's step depth.
     * @param alias           The column's alias.
     * @param foreignKeyParts The foreign key column broken up into its parts. schema, table and for user supplied identifiers the property name.
     */
    private void addForeignKey(String schema, String table, String column, int stepDepth, String alias, String[] foreignKeyParts) {
        Column c = add(schema, table, column, stepDepth, alias, null);
        c.isForeignKey = true;
        if (foreignKeyParts.length == 3) {
            Map<String, PropertyType> properties = this.filteredAllTables.get(foreignKeyParts[0] + "." + Topology.VERTEX_PREFIX + foreignKeyParts[1]);
            if (foreignKeyParts[2].endsWith(Topology.IN_VERTEX_COLUMN_END)) {
                c.propertyType = properties.get(foreignKeyParts[2].substring(0, foreignKeyParts[2].length() - Topology.IN_VERTEX_COLUMN_END.length()));
                c.foreignKeyDirection = Direction.IN;
                c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1]);
                c.foreignKeyProperty = foreignKeyParts[2];
            } else {
                c.propertyType = properties.get(foreignKeyParts[2].substring(0, foreignKeyParts[2].length() - Topology.OUT_VERTEX_COLUMN_END.length()));
                c.foreignKeyDirection = Direction.OUT;
                c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1]);
                c.foreignKeyProperty = foreignKeyParts[2];
            }
        } else {
            c.propertyType = PropertyType.LONG;
            c.foreignKeyDirection = (column.endsWith(Topology.IN_VERTEX_COLUMN_END) ? Direction.IN : Direction.OUT);
            c.foreignSchemaTable = SchemaTable.of(foreignKeyParts[0], foreignKeyParts[1].substring(0, foreignKeyParts[1].length() - Topology.IN_VERTEX_COLUMN_END.length()));
            c.foreignKeyProperty = null;
        }
    }


    void addForeignKey(SchemaTableTree stt, String column, String alias) {
        String[] foreignKeyParts = column.split("\\.");
        Preconditions.checkState(foreignKeyParts.length == 2 || foreignKeyParts.length == 3, "Edge table foreign must be schema.table__I\\O or schema.table.property__I\\O. Found %s", column);
        addForeignKey(stt.getSchemaTable().getSchema(), stt.getSchemaTable().getTable(), column, stt.getStepDepth(), alias, foreignKeyParts);
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param schema
     * @param table
     * @param column
     * @return
     */
    private String getAlias(String schema, String table, String column, int stepDepth, String aggregateFunction) {
        //PropertyType is not part of equals or hashCode so not needed for the lookup.
        Column c = new Column(schema, table, column, null, stepDepth, aggregateFunction);
        return columns.get(c);
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param stt
     * @param column
     * @return
     */
    String getAlias(SchemaTableTree stt, String column) {
        Pair<String, List<String>> aggregateFunction = stt.getAggregateFunction();
        if (aggregateFunction == null) {
            return getAlias(
                    stt.getSchemaTable(),
                    column,
                    stt.getStepDepth(),
                    null
            );
        } else {
            if (aggregateFunction.getRight().isEmpty() || aggregateFunction.getRight().contains(column)) {
                return getAlias(
                        stt.getSchemaTable(),
                        column,
                        stt.getStepDepth(),
                        stt.getAggregateFunction().getLeft()
                );
            } else {
                return getAlias(
                        stt.getSchemaTable(),
                        column,
                        stt.getStepDepth(),
                        null
                );
            }
        }
    }

    /**
     * get an alias if the column is already in the list
     *
     * @param st
     * @param column
     * @param stepDepth
     * @return
     */
    String getAlias(SchemaTable st, String column, int stepDepth, String aggregateFunction) {
        return getAlias(st.getSchema(), st.getTable(), column, stepDepth, aggregateFunction);
    }

    String toFromStatement(boolean partOfDuplicateQuery) {
        String sep = "";
        StringBuilder sb = new StringBuilder();
        Set<Column> tmpColumns = new LinkedHashSet<>();
        if (!partOfDuplicateQuery && this.containsAggregate) {
            this.columns.keySet().stream().filter(c -> !c.isID && !c.isForeignKey).forEach(tmpColumns::add);
        } else {
            tmpColumns.addAll(this.columns.keySet());
        }
        for (Column c : tmpColumns) {
            String alias = this.columns.get(c);
            sb.append(sep);
            sep = ",\n\t";
            c.toFromStatement(sb, partOfDuplicateQuery);
            sb.append(" AS ");
            sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
            if (this.drop) {
                break;
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toOuterFromStatement("", false);
    }

    @SuppressWarnings("Duplicates")
    String toOuterFromStatement(String prefix, boolean stackContainsAggregate) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        List<String> fromAliases = this.aliases.keySet().stream().filter(
                (alias) -> !alias.endsWith(Topology.IN_VERTEX_COLUMN_END) && !alias.endsWith(Topology.OUT_VERTEX_COLUMN_END))
                .collect(Collectors.toList());
        boolean first = true;
        for (String alias : fromAliases) {
            Column c = this.aliases.get(alias);
            if (stackContainsAggregate && !c.isID && c.aggregateFunction != null) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(c.aggregateFunction);
                sb.append("(");
                sb.append(prefix);
                sb.append(".");
                sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
                sb.append(")");
            } else if (!stackContainsAggregate) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(prefix);
                sb.append(".");
                sb.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(alias));
            }
            i++;
        }
        return sb.toString();
    }

    public Pair<String, PropertyType> getPropertyType(String alias) {
        Column column = this.aliases.get(alias);
        if (column != null) {
            return Pair.of(column.column, column.propertyType);
        } else {
            return null;
        }
    }

    Map<SchemaTable, List<Column>> getInForeignKeys(int stepDepth, SchemaTable schemaTable) {
        return getForeignKeys(stepDepth, schemaTable, Direction.IN);
    }

    Map<SchemaTable, List<Column>> getOutForeignKeys(int stepDepth, SchemaTable schemaTable) {
        return getForeignKeys(stepDepth, schemaTable, Direction.OUT);
    }

    private Map<SchemaTable, List<Column>> getForeignKeys(int stepDepth, SchemaTable schemaTable, Direction direction) {
        Map<SchemaTable, List<Column>> result = new HashMap<>();
        for (Column column : this.columns.keySet()) {
            if (column.isForeignKey && column.foreignKeyDirection == direction && column.isFor(stepDepth, schemaTable)) {
                List<Column> columns = result.computeIfAbsent(column.getForeignSchemaTable(), (k) -> new ArrayList<>());
                columns.add(column);
            }
        }
        return result;
    }

    LinkedHashMap<Column, String> getFor(int stepDepth, SchemaTable schemaTable) {
        LinkedHashMap<Column, String> result = new LinkedHashMap<>();
        for (Column column : this.columns.keySet()) {
            if (column.isFor(stepDepth, schemaTable)) {
                result.put(column, this.columns.get(column));
            }
        }
        return result;
    }

    void indexColumns(int startColumnIndex) {
        int i = startColumnIndex;
        for (Column column : columns.keySet()) {
            if (!this.containsAggregate) {
                column.columnIndex = i++;
            } else if (!column.isID && !column.isForeignKey) {
                column.columnIndex = i++;
            }
        }
    }

    int indexColumnsExcludeForeignKey(int startColumnIndex, boolean stackContainsAggregate) {
        int i = startColumnIndex;
        for (String alias : this.aliases.keySet()) {
            Column column = this.aliases.get(alias);
            if (!alias.endsWith(Topology.IN_VERTEX_COLUMN_END) && !alias.endsWith(Topology.OUT_VERTEX_COLUMN_END) &&
                    (!stackContainsAggregate || (!column.isID && column.aggregateFunction != null))) {
                this.aliases.get(alias).columnIndex = i++;
            }
        }
        return i;
    }

    /**
     * simple column, fully qualified: schema+table+column
     *
     * @author jpmoresmau
     */
    public class Column {
        private final String schema;
        private final String table;
        private final String column;
        private final int stepDepth;
        private PropertyType propertyType;
        private final boolean isID;
        private int columnIndex = -1;

        //Foreign key properties
        private boolean isForeignKey;
        private Direction foreignKeyDirection;
        private SchemaTable foreignSchemaTable;
        //Only set for user identifier primary keys
        private String foreignKeyProperty;

        private String aggregateFunction;

        Column(String schema, String table, String column, PropertyType propertyType, int stepDepth, String aggregateFunction) {
            super();
            this.schema = schema;
            this.table = table;
            this.column = column;
            this.propertyType = propertyType;
            this.stepDepth = stepDepth;
            this.isID = this.column.equals(Topology.ID);
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((column == null) ? 0 : column.hashCode());
            result = prime * result + ((schema == null) ? 0 : schema.hashCode());
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + ((aggregateFunction == null) ? 0 : aggregateFunction.hashCode());
            result = prime * result + stepDepth;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Column other = (Column) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (column == null) {
                if (other.column != null)
                    return false;
            } else if (!column.equals(other.column))
                return false;
            if (schema == null) {
                if (other.schema != null)
                    return false;
            } else if (!schema.equals(other.schema))
                return false;
            if (table == null) {
                if (other.table != null)
                    return false;
            } else if (!table.equals(other.table))
                return false;
            if (aggregateFunction == null) {
                if (other.aggregateFunction != null)
                    return false;
            } else if (!aggregateFunction.equals(other.aggregateFunction))
                return false;
            return this.stepDepth == other.stepDepth;
        }

        private ColumnList getOuterType() {
            return ColumnList.this;
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public String getColumn() {
            return column;
        }

        public int getStepDepth() {
            return stepDepth;
        }

        public PropertyType getPropertyType() {
            return propertyType;
        }

        public boolean isID() {
            return isID;
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public boolean isForeignKey() {
            return isForeignKey;
        }

        public Direction getForeignKeyDirection() {
            return foreignKeyDirection;
        }

        public SchemaTable getForeignSchemaTable() {
            return foreignSchemaTable;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        boolean isForeignKeyProperty() {
            return foreignKeyProperty != null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toFromStatement(sb, false);
            return sb.toString();
        }

        /**
         * to string using provided builder
         *
         * @param sb
         */
        void toFromStatement(StringBuilder sb, boolean partOfDuplicateQuery) {
            if (!this.isID && !partOfDuplicateQuery && this.aggregateFunction != null) {
                sb.append(this.aggregateFunction.equals(GraphTraversal.Symbols.mean) ? "avg" : this.aggregateFunction);
                sb.append("(");
            }
            sb.append(ColumnList.this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sb.append(".");
            sb.append(ColumnList.this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sb.append(".");
            sb.append(ColumnList.this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
            if (!this.isID && !partOfDuplicateQuery && this.aggregateFunction != null) {
                sb.append(")");
            }
        }

        public String getAggregateFunction() {
            return aggregateFunction;
        }

        boolean isFor(int stepDepth, SchemaTable schemaTable) {
            return this.stepDepth == stepDepth && this.schema.equals(schemaTable.getSchema()) && this.table.equals(schemaTable.getTable());
        }

        public boolean isForeignKey(int stepDepth, SchemaTable schemaTable) {
            return this.stepDepth == stepDepth && this.schema.equals(schemaTable.getSchema()) && this.table.equals(schemaTable.getTable());
        }

    }
}
