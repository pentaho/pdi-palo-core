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

import com.jedox.palojlib.interfaces.ICube;
import com.jedox.palojlib.interfaces.IDatabase;
import com.jedox.palojlib.interfaces.IDimension;
import com.jedox.palojlib.interfaces.IElement;

public class PaloCubeCache {
	private final IDatabase database;
	private final IDimension [] dimensions;
	private final ICube cube;
	private final ArrayList<PaloDimensionCache> cubeCache = new ArrayList<PaloDimensionCache>();
	private final boolean enableCache;
	
	public PaloCubeCache(PaloHelper helper, final String cubeName, boolean enableCache) throws Exception {
		this.database = helper.getDatabase();
		this.cube = database.getCubeByName(cubeName);
		this.enableCache = enableCache;
		
		if(this.cube == null)
            throw new Exception("The cube "+cubeName+" does not exist.");
		
		this.dimensions = cube.getDimensions();
    }
	
	public void loadCubeCache() throws Exception {
		if (!enableCache)
			throw new Exception("Cache is not enabled");
	
		cubeCache.clear();
		for (IDimension d : cube.getDimensions()) {
			PaloDimensionCache dimensionCache = new PaloDimensionCache(database,d,this.enableCache);
			dimensionCache.loadDimensionCache();
        	cubeCache.add(dimensionCache);
        }
	}
	
	public IElement getElement(int dimensionIndex, final String dimensionName){
		IElement element = null;
		
		/* The dimension cache needs to have the same order/indexing as the cube
		 * Add blanks to fill any spaces if cache wasn't pre-loaded */
		if (cubeCache.size() <= dimensionIndex)
			for(IDimension d : cube.getDimensions())
				cubeCache.add(new PaloDimensionCache(database,d,this.enableCache));
		
		element = cubeCache.get(dimensionIndex).getElement(dimensionName);
		
		return element;
	}
	
	public ICube getCube(){
		return cube;
	}
	
	public String getCubeName(){
		return cube.getName();
	}

	public IDimension [] getDimensions() {
		return dimensions;
	}
}
