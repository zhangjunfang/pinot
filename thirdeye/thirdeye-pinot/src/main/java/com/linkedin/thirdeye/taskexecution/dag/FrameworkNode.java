package com.linkedin.thirdeye.taskexecution.dag;

/**
 * Execution Framework Related Methods.
 *
 * @param <T> the type that extends this interface.
 */
public interface FrameworkNode<T extends FrameworkNode> extends Node<T> {

  FrameworkNode getLogicalParentNode();

  FrameworkNode getLogicalChildNode();

}
