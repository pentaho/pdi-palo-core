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
 *   Copyright 2011 De Bortoli Wines Pty Limited (Australia)
 */

/**
 * This Class manages dimension cache for a cube to minimize palo calls to the server. 
 * 
 * @author Pieter van der Merwe
 * @since 05-08-2011
 */

package org.pentaho.di.palo.core;

import java.util.ArrayList;

import org.palo.api.Cube;
import org.palo.api.Database;
import org.palo.api.Dimension;
import org.palo.api.Element;

public class PaloCubeCache {
	private Cube cube;
	private ArrayList<PaloDimensionCache> cubeCache = new ArrayList<PaloDimensionCache>();
	private boolean enableCache = false;
	
	public PaloCubeCache(PaloHelper helper, final String cubeName, boolean enableCache) throws Exception {
		this.cube = helper.getDatabase().getCubeByName(cubeName);
    	this.enableCache = enableCache;
    	
    	if(this.cube == null)
            throw new Exception("The cube "+cubeName+" does not exist.");
    }
	
	public PaloCubeCache(Database paloDatabase, final String cubeName, boolean enableCache) throws Exception {
    	this.cube = paloDatabase.getCubeByName(cubeName);
    	this.enableCache = enableCache;
    	
    	if(this.cube == null)
            throw new Exception("The cube "+cubeName+" does not exist.");
    }
	
	public void loadCubeCache() throws Exception {
		if (!enableCache)
			throw new Exception("Cache is not enabled");
	
		cubeCache.clear();
		for (int j = 0; j < cube.getDimensionCount(); j++) {
            Dimension d = cube.getDimensionAt(j);
            PaloDimensionCache dimensionCache = new PaloDimensionCache(cube.getDatabase(),d,this.enableCache);
        	cubeCache.add(dimensionCache);
        }
	}
	
	public Element getElement(int dimensionIndex, final String dimensionName){
		Element element = null;
		
		/* The dimension cache needs to have the same order/indexing as the cube
		 * Add blanks to fill any spaces if cache wasn't pre-loaded */
		if (cubeCache.size() <= dimensionIndex)
			for(int i = cubeCache.size(); i <= dimensionIndex; i++)
				cubeCache.add(new PaloDimensionCache(cube.getDatabase(),cube.getDimensionAt(i),this.enableCache));
		
		element = cubeCache.get(dimensionIndex).getElement(dimensionName);
		
		return element;
	}
	
	public Cube getCube(){
		return cube;
	}
	
	public String getCubeName(){
		return cube.getName();
	}
}
