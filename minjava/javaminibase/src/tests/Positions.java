package tests;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import bitmap.*;
import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import diskmgr.*;

public class Positions{

    int position;
    ArrayList<RID> Rids=null;
    public Positions(){
        ArrayList<RID> Rids=new ArrayList<RID>();
    }

    public Positions(int position){
        this.position=position;
        ArrayList<RID> Rids=new ArrayList<RID>();
    }
    public Positions(int position,ArrayList<RID> Rids){
        this.position = position;
        this.Rids = Rids;
    }
    public int getposition(){
        return this.position;
    }
    public ArrayList<RID> getRids(){
        return this.Rids;
    }
}