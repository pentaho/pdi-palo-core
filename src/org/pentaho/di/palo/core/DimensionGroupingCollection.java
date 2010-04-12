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

import java.util.ArrayList;

/**
 * Data typing of a list of groups of dimensions
 */
public class DimensionGroupingCollection extends ArrayList<DimensionGrouping> {
	
    /**
	 * 
	 */
	private static final long	serialVersionUID	= 8285613250561692075L;

	/**
     * Creates an empty collection of dimension grouppings.
     */
    public DimensionGroupingCollection() {
        super();
    }

    /**
     * Tests if dimension with given name is in the group.
     */
	public final boolean contains(final String name) {
		for (final DimensionGrouping c : this) {
			if (c.getName().equals(name)) 
			    return true;
		}
		return false;
	}
	
	
	public final DimensionGrouping find(final Object name) {
	    for (final DimensionGrouping c : this) {
                if (c.getName().equals(name)) return c;
            }
            return null;
	}
	
	public boolean add(DimensionGrouping dimensionGrouping) {
	    for (final DimensionGrouping c : this) {
                if (c.getLevel() != (dimensionGrouping.getLevel())) return false;
            }
	    this.add(this.size(), dimensionGrouping);
            return true;
	    
	}
}


