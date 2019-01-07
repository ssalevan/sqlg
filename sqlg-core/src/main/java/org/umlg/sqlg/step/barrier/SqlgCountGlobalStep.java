package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/01/04
 */
public class SqlgCountGlobalStep extends SqlgAbstractStep<SqlgElement, Long> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.BULK);
    public static final String SQLG_COUNT = "sqlg_count";
    private long count = 0;
    private boolean returned = false;

    public SqlgCountGlobalStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        while (this.starts.hasNext()) {
            Traverser.Admin<SqlgElement> s = this.starts.next();
            SqlgElement sqlgElement = s.get();
            this.count += sqlgElement.<Long>value(SQLG_COUNT);
        }
        if (!this.returned) {
            this.returned = true;
            return this.getTraversal().getTraverserGenerator().generate(this.count, (Step) this, 1l);
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
    
    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public void reset() {
        this.count = 0;
        this.returned = false;
    }
}
