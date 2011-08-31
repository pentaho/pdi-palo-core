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
 * This Class manages dimension cache to minimize palo calls to the server. 
 * 
 * @author Pieter van der Merwe
 * @since 05-08-2011
 */
package org.pentaho.di.palo.core;

import java.util.Hashtable;

import org.palo.api.Database;
import org.palo.api.Dimension;
import org.palo.api.Element;

public class PaloDimensionCache {
	private Hashtable<String, Element> elementCache = new Hashtable<String, Element>();
	private String dimensionName = "";
	private Dimension dimension;
	private boolean enableCache = false;
	private boolean allLoaded = false;
	
	public PaloDimensionCache(Database paloDatabase, Dimension dimension, boolean enableCache){
		this.dimension = dimension;
		this.dimensionName = this.dimension.getName();
		this.enableCache = enableCache;
	}
	
	public PaloDimensionCache(Database paloDatabase, String dimensionName, boolean enableCache) throws Exception{
		this.dimensionName = dimensionName;
		this.enableCache = enableCache;
		
		this.dimension = paloDatabase.getDimensionByName(dimensionName);
		
		if(this.dimension == null)
            throw new Exception("The dimension "+dimensionName+" does not exist.");
	}
	
	public void loadDimensionCache() throws Exception {
		if (!enableCache)
			throw new Exception("Cache is not enabled for dimension " + dimensionName);

		Element [] elements = dimension.getDefaultHierarchy().getElements();

		for (Element elem : elements){
			elementCache.put(elem.getName(), elem);
		}
		
		allLoaded = true;
	}
	
	public Element getElement(final String elementName){
		Element element = null;
		
		if (enableCache)
			element = elementCache.get(elementName);
		
		if (element == null && allLoaded == false)
		{
			element = dimension.getDefaultHierarchy().getElementByName(elementName);
			if (enableCache && element != null)
				elementCache.put(elementName,element);
		}
		
		return element;
	}
	
	public Element createElement(String elementName, int elementType, boolean errorIfExists) throws Exception{
		Element elem = getElement(elementName);
		if (errorIfExists == true && elem != null)
			throw new Exception("Element with name " + elementName + " already exists");
		
		/* New item */
		if (elem == null){
			elem = dimension.getDefaultHierarchy().addElement(elementName, elementType);
			if (enableCache)
				elementCache.put(elementName,elem);
		}
		return elem;
	}
	
	public String getDimensionName(){
		return dimensionName;
	}
	
	public Dimension getDimension(){
		return dimension;
	}
}
