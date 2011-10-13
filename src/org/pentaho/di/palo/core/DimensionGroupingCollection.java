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
 *   GNU Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License
 *   along with PaloKettlePlugin.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   Copyright 2008 Stratebi Business Solutions, S.L.
 *   Copyright 2010 Pentaho
 *   Copyright 2011 De Bortoli Wines Pty Limited (Australia)
 */

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Data typing of a list of groups of dimensions
 */
public class DimensionGroupingCollection extends ArrayList<DimensionGrouping> {
	
	// Pieter vd Merwe - Can't make the base class extend HashMap since we need the order of the ArrayList
	private HashMap<String, DimensionGrouping> fastLookup = new HashMap<String, DimensionGrouping>();
	private int level = -1;
    
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
    
    private String buildKey(final String name, final double consolidationFactor){
		return name.toString() + "|" + Double.toString(consolidationFactor);
	}
	
	/**
	 * Tests if dimension with given name is in the group.
	 */
	public final boolean contains(final String name, final double consolidationFactor) {
		return fastLookup.containsKey(buildKey(name,consolidationFactor));
	}
	
	
	public final DimensionGrouping find(final Object name, final double consolidationFactor) {
		String key = buildKey(name.toString(),consolidationFactor);
		if (fastLookup.containsKey(key))
			return fastLookup.get(key);
		else return null;
	}

	public boolean add(DimensionGrouping dimensionGrouping) {
		String key = buildKey(dimensionGrouping.getName(),dimensionGrouping.getConsolidationFactor());
		
		if (level == -1)
			level = dimensionGrouping.getLevel();
		else if (level != (dimensionGrouping.getLevel())) return false;
	    
	    this.add(this.size(), dimensionGrouping);
	    fastLookup.put(key, dimensionGrouping);

	    return true;
	    
	}
}