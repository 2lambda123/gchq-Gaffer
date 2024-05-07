package uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopVertexStep;

public final class GafferPopVertexStepStrategy<E extends Element> extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final GafferPopVertexStepStrategy INSTANCE = new GafferPopVertexStepStrategy();

  private GafferPopVertexStepStrategy() { }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(original -> {
      GafferPopVertexStep<?> newVertexStep = new GafferPopVertexStep<>(original);
      FoldStep<Vertex, Vertex> foldStep = new FoldStep<>(traversal);
      TraversalHelper.replaceStep(original, foldStep, traversal);
      TraversalHelper.insertAfterStep(newVertexStep, foldStep, traversal);
    });
  }

  public static GafferPopVertexStepStrategy instance() {
    return INSTANCE;
  }
}
