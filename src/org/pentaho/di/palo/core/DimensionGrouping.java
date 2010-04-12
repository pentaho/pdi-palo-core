package org.pentaho.di.palo.core;
/*
 *   This file is part of PaloKettlePlugin.
 *
 *   PaloKettlePlugin is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Lesser General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   PaloKettlePlugin is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   Copyright 2008 Stratebi Business Solutions, S.L.
 *   Copyright 2010 Pentaho
 */


/**
 * Group of elements for a dimension.
 */
public class DimensionGrouping {
	private final String id;
	private final String name;
	private final int level;
	private final DimensionGroupingCollection children;

    /**
     * Creates a group if dimensions
     */
	
	public DimensionGrouping(final String name, final String id) {
	    this.name = name;
	    this.id = id;
	    this.level = 0;
	    this.children = new DimensionGroupingCollection();
	}
	public DimensionGrouping(final String name, final String id, 
            final int level) {
		this.name = name;
		this.id = id;
		this.level = level;
                this.children = new DimensionGroupingCollection();
	}

    public final String getId() { return this.id; }

    public final String getName() { return this.name; }
    
    public final int getLevel() { return this.level; }

    //public DimensionGroupingCollection getParents() { return this.parents; }

    

    /**
     * Returns an immutable list of children.
     */
    public final DimensionGroupingCollection getChildren() {
        return new ReadOnlyDimensionGroupingCollection(this.children);
    }

    //this is ok but i need to get DimensionGroupingCollection
    //public final List < DimensionGrouping > getChildren() {
    //    return Collections.unmodifiableList(this.children);
    //}

    /**
     * Tests if child with given name is contained into the list of
     * recognized children.
     */
    public final boolean containsChild(final String name) {
        return this.children.contains(name);     
    }
    public final DimensionGrouping findChild(final String name) {
        return this.children.find(name);
    }
    public final void addChild(final DimensionGrouping child) throws Exception {
        if (this.level != child.level + 1)
            throw new Exception("Level of the child must be level of the parent - 1");
        this.children.add(child);
    }

    public final boolean equals(final Object o) {
        if (o instanceof DimensionGrouping) {
            return this.id.equals(((DimensionGrouping) o).id);
        } else return false;
    }

    public final int hashCode() {
        return id.hashCode();
    }
}
