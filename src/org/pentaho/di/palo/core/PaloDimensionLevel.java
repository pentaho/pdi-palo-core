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

public class PaloDimensionLevel {
    private String LevelName;
    private int LevelNumber;
    private String FieldName;
    private String FieldType;
    
    public PaloDimensionLevel(String levelName, int levelNumber, String fieldName, String fieldType) {
        this.LevelName = levelName;
        this.LevelNumber = levelNumber;
        this.FieldName = fieldName;
        this.FieldType = fieldType;
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
}
