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
*   Portions Copyright 2008 Stratebi Business Solutions, S.L.
*   Portions Copyright 2011 De Bortoli Wines Pty Limited (Australia)
*   Portions Copyright 2010 - 2013 Pentaho Corporation
*/

package org.pentaho.di.palo.core;

public class PaloDimensionLevel {
    private String LevelName;
    private int LevelNumber;
    private String FieldName;
    private String FieldType;
    private String ConsolidationFieldName;
    
    public PaloDimensionLevel(String levelName, int levelNumber, String fieldName, String fieldType) {
    	this(levelName,levelNumber,fieldName,fieldType,null);
    }
    
    public PaloDimensionLevel(String levelName, int levelNumber, String fieldName, String fieldType, String ConsolidationFieldName) {
        this.LevelName = levelName;
        this.LevelNumber = levelNumber;
        this.FieldName = fieldName;
        this.FieldType = fieldType;
        this.ConsolidationFieldName = ConsolidationFieldName;
    }
    public String getLevelName() {
        return this.LevelName;
    }
    public String getFieldName() {
        return this.FieldName;
    }
    public String getFieldType() {
        return this.FieldType;
    }
    public int getLevelNumber() {
        return this.LevelNumber;
    }
    public String getConsolidationFieldName(){
    	return this.ConsolidationFieldName;
    }
}
