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
 *   Copyright 2012 De Bortoli Wines Pty Limited (Australia)
 */

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * 
 * @author Pieter van der Merwe
 * @since 02 March 2012
 */
public class ConsolidationElement {
	private final String name;
	private boolean isChild = false;
	private final ArrayList<Consolidation> children;
	private final Hashtable<String, Consolidation> childrenFast;
	
	public ConsolidationElement(String name){
		this.name = name;
		children = new ArrayList<Consolidation>();
		childrenFast = new Hashtable<String, Consolidation>();
	}
	
	public String getName() {
		return name;
	}
	
	public ArrayList<Consolidation> getChildren() {
		return children;
	}
	
	public void setIsChild() {
		this.isChild = true;
	}
	
	public boolean getIsChild() {
		return this.isChild;
	}
	
	public void addChild(ConsolidationElement element, double consolidationFactor){
		Consolidation child;
		
		// Children must be unique.  
		if (!childrenFast.containsKey(element.getName())){
			child = new Consolidation(element, consolidationFactor);
			
			children.add(child);
			childrenFast.put(element.getName(), child);
			element.setIsChild();
		}
		else{
			child = childrenFast.get(element.getName());
		}
		
		// If the item existed before, overwrite the consolidation factor with the last update
		child.setConsolidationFactor(consolidationFactor);
		
	}
	
	public class Consolidation {
		private final ConsolidationElement element;
		private double consolidationFactor;
		
		public Consolidation(ConsolidationElement element, double consolidationFactor){
			this.element = element;
			this.consolidationFactor = consolidationFactor;
		}
		
		public ConsolidationElement getElement() {
			return element;
		}
		
		public double getConsolidationFactor() {
			return consolidationFactor;
		}
		
		public void setConsolidationFactor(double consolidationFactor) {
			this.consolidationFactor = consolidationFactor;
		}
	}
}
