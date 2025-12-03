
const DataTypes = {
  sqlserver:[
    "bigint","int","smallint","tinyint","bit","decimal","decimal(18,2)","numeric","money","smallmoney","float","real","date","datetime","datetime2","smalldatetime","datetimeoffset","time","char","char(10)","varchar","varchar(255)","varchar(max)","text","nchar","nchar(10)","nvarchar","nvarchar(255)","nvarchar(max)","ntext","binary","varbinary","varbinary(max)","image","rowversion","timestamp","uniqueidentifier","sql_variant","xml","cursor","table","geometry","geography"
  ],
  mysql:[
    "TINYINT","SMALLINT","MEDIUMINT","INT","INTEGER","BIGINT","DECIMAL","DECIMAL(10,2)","NUMERIC","FLOAT","DOUBLE","DOUBLE PRECISION","BIT","BOOL","BOOLEAN","DATE","DATETIME","TIMESTAMP","TIME","YEAR","CHAR","CHAR(10)","VARCHAR","VARCHAR(255)","TINYTEXT","TEXT","MEDIUMTEXT","LONGTEXT","BINARY","VARBINARY","VARBINARY(255)","TINYBLOB","BLOB","MEDIUMBLOB","LONGBLOB","ENUM","SET","GEOMETRY","POINT","LINESTRING","POLYGON","MULTIPOINT","MULTILINESTRING","MULTIPOLYGON","GEOMETRYCOLLECTION","JSON"
  ],
  postgres:[
    "smallint","integer","int","bigint","decimal","decimal(10,2)","numeric","real","double precision","smallserial","serial","bigserial","money","char","character","character(10)","varchar","varchar(255)","character varying","text","bytea","timestamp","timestamp without time zone","timestamp with time zone","timestamptz","date","time","interval","boolean","enum","array","json","jsonb","uuid","xml","int4range","int8range","numrange","tsrange","tstzrange","daterange","point","line","lseg","box","path","polygon","circle","cidr","inet","macaddr","macaddr8","tsvector","tsquery","oid","regclass","pg_lsn"
  ],
  sqlite:[
    "NULL","INTEGER","REAL","TEXT","BLOB","NUMERIC (affinity)","INT","TINYINT","SMALLINT","MEDIUMINT","BIGINT","UNSIGNED BIG INT","INT2","INT8","CHARACTER","VARCHAR","VARCHAR(255)","VARYING CHARACTER","NCHAR","NVARCHAR","CLOB","DOUBLE","DOUBLE PRECISION","FLOAT","BOOLEAN","DATE","DATETIME"
  ]
};
const DbVersions={
  sqlserver:"2025",
  mysql:"9.2.0 (Innovation) / 8.4.4 (LTS)",
  postgres:"17.4",
  sqlite:"3.51.1"
};
function uuid(){if(crypto.randomUUID)return crypto.randomUUID();return"id-"+Math.random().toString(36).slice(2)+Date.now().toString(36)}
const State={
  dbType:"mysql",
  dbVersion:"9.2.0 (Innovation) / 8.4.4 (LTS)",
  dbName:"my_database",
  schemas:[],
  tables:[],
  relations:[],
  selectedTableIds:new Set(),
  selectedFieldId:null,
  selectedRelationId:null,
  zoom:1,
  offsetX:80,
  offsetY:80,
  panMode:false,
  interactionMode:"move",
  displayMode:"en",
  themeMode:"dark",
  themeColors:{
    bgA:"#010409",bgB:"#0b1220",
    headerA:"#020617",headerB:"#0b1327",
    panelA:"#0a0f1d",panelB:"#0f172a",
    text:"#e5e7eb",panelBorder:"#0d1117"
  }
};
const Views={
  login:document.getElementById("loginView"),
  config:document.getElementById("configView"),
  main:document.getElementById("mainView")
};
const Login={
  user:document.getElementById("loginUser"),
  pass:document.getElementById("loginPass"),
  btn:document.getElementById("loginBtn"),
  error:document.getElementById("loginError")
};
const Config={
  back:document.getElementById("cfgBack"),
  cont:document.getElementById("cfgContinue"),
  dbName:document.getElementById("cfgDbName"),
  dbType:document.getElementById("cfgDbType"),
  dbVersion:document.getElementById("cfgDbVersion"),
  jsonFile:document.getElementById("cfgJsonFile"),
  sqlFile:document.getElementById("cfgSqlFile"),
  status:document.getElementById("cfgStatus")
};
function populateDbVersions(){
  if(!Config.dbVersion)return;
  Config.dbVersion.innerHTML="";
  const v=DbVersions[Config.dbType.value]||"latest";
  const opt=document.createElement("option");
  opt.value=v;
  opt.textContent=v;
  Config.dbVersion.appendChild(opt);
}
const Header={
  dbName:document.getElementById("headerDbName"),
  dbType:document.getElementById("headerDbType"),
  dbVersion:document.getElementById("headerDbVersion"),
  subtitle:document.getElementById("headerSubtitle"),
  displayEnBtn:document.getElementById("displayEnBtn"),
  displayFaBtn:document.getElementById("displayFaBtn"),
  pinBtn:document.getElementById("headerPinBtn"),
  container:document.getElementById("mainHeader"),
  content:document.getElementById("headerContent")
};
const LeftPanel={
  jsonArea:document.getElementById("jsonArea"),
  sqlArea:document.getElementById("sqlArea"),
  copyJsonBtn:document.getElementById("copyJsonBtn"),
  copySqlBtn:document.getElementById("copySqlBtn"),
  importJsonBtn:document.getElementById("importJsonBtn"),
  applySqlBtn:document.getElementById("applySqlBtn"),
  exportJsonBtn:document.getElementById("exportJsonBtn"),
  exportSqlBtn:document.getElementById("exportSqlBtn")
};
const Canvas={
  pan:document.getElementById("canvasPan"),
  inner:document.getElementById("canvasInner"),
  relationLayer:document.getElementById("relationLayer"),
  zoomInBtn:document.getElementById("zoomInBtn"),
  zoomOutBtn:document.getElementById("zoomOutBtn"),
  zoomResetBtn:document.getElementById("zoomResetBtn"),
  panModeBtn:document.getElementById("panModeBtn"),
  zoomIndicator:document.getElementById("zoomIndicator"),
  statusSelection:document.getElementById("statusSelection"),
  statusZoom:document.getElementById("statusZoom"),
  modeSelectBtn:document.getElementById("modeSelectBtn"),
  modeMoveBtn:document.getElementById("modeMoveBtn"),
  modePanBtn:document.getElementById("modePanBtn")
};
const Appearance={
  colorBgA:document.getElementById("colorBgA"),
  colorBgB:document.getElementById("colorBgB"),
  colorHeaderA:document.getElementById("colorHeaderA"),
  colorHeaderB:document.getElementById("colorHeaderB"),
  colorPanelA:document.getElementById("colorPanelA"),
  colorPanelB:document.getElementById("colorPanelB"),
  colorText:document.getElementById("colorText"),
  applyBtn:document.getElementById("applyThemeBtn"),
  resetBtn:document.getElementById("resetThemeBtn"),
  themeDarkBtn:document.getElementById("themeDarkBtn"),
  themeLightBtn:document.getElementById("themeLightBtn"),
  autoSortBtn:document.getElementById("autoSortBtn"),
  editDbNameBtn:document.getElementById("editDbNameBtn")
};
const RightPanel={
  tableSelect:document.getElementById("tableSelect"),
  newTableBtn:document.getElementById("newTableBtn"),
  deleteBtn:document.getElementById("deleteBtn"),
  tblName:document.getElementById("tblName"),
  tblSchemaSelect:document.getElementById("tblSchemaSelect"),
  tblFaName:document.getElementById("tblFaName"),
  tblDeleteBehavior:document.getElementById("tblDeleteBehavior"),
  tblDescription:document.getElementById("tblDescription"),
  addFieldBtn:document.getElementById("addFieldBtn"),
  fldName:document.getElementById("fldName"),
  fldFaName:document.getElementById("fldFaName"),
  fldType:document.getElementById("fldType"),
  fldTypeParams:document.getElementById("fldTypeParams"),
  fldNullable:document.getElementById("fldNullable"),
  fldPrimary:document.getElementById("fldPrimary"),
  fldDescription:document.getElementById("fldDescription"),
  fldPinned:document.getElementById("fldPinned"),
  moveFieldUp:document.getElementById("moveFieldUp"),
  moveFieldDown:document.getElementById("moveFieldDown"),
  relFromTable:document.getElementById("relFromTable"),
  relFromField:document.getElementById("relFromField"),
  relToTable:document.getElementById("relToTable"),
  relToField:document.getElementById("relToField"),
  relOnDelete:document.getElementById("relOnDelete"),
  addRelationBtn:document.getElementById("addRelationBtn"),
  relationsList:document.getElementById("relationsList"),
  relSourceLabel:document.getElementById("relSourceLabel"),
  relCenterLabel:document.getElementById("relCenterLabel"),
  relTargetLabel:document.getElementById("relTargetLabel"),
  relStyle:document.getElementById("relStyle"),
  relColorA:document.getElementById("relColorA"),
  relColorB:document.getElementById("relColorB"),
  relBend:document.getElementById("relBend"),
  schemaList:document.getElementById("schemaList"),
  addSchemaBtn:document.getElementById("addSchemaBtn"),
  saveSchemaBtn:document.getElementById("saveSchemaBtn"),
  deleteSchemaBtn:document.getElementById("deleteSchemaBtn"),
  schemaName:document.getElementById("schemaName"),
  schemaFaName:document.getElementById("schemaFaName"),
  schemaColorA:document.getElementById("schemaColorA"),
  schemaColorB:document.getElementById("schemaColorB")
};
const Controls={logoutBtn:document.getElementById("logoutBtn")};
const PanelUI={
  side:document.getElementById("sidePanel"),
  scroll:document.getElementById("sidePanelScroll"),
  toggleBtn:document.getElementById("panelToggleHeader"),
  accordions:Array.from(document.querySelectorAll(".accordion-toggle")),
  sections:Array.from(document.querySelectorAll(".accordion")).map(el=>({
    element:el,
    toggle:el.querySelector(".accordion-toggle"),
    body:null,
    pinBtn:el.querySelector(".accordion-pin"),
    handle:el.querySelector(".accordion-handle")
  }))
};
const OptionUI={
  panel:document.getElementById("optionPanel"),
  toggleBtn:document.getElementById("optionToggleBtn"),
  zoomInBtn:document.getElementById("optionZoomIn"),
  zoomOutBtn:document.getElementById("optionZoomOut"),
  zoomResetBtn:document.getElementById("optionZoomReset"),
  selectBtn:document.getElementById("optionSelectBtn"),
  moveBtn:document.getElementById("optionMoveBtn"),
  panBtn:document.getElementById("optionPanBtn"),
  handle:document.querySelector(".option-panel-handle")
};
let optionPanelInitialized=false;
function clampWithinParent(el,x,y,pad=0){
  const parent=el?.offsetParent||document.body;
  const maxX=(parent?.clientWidth||window.innerWidth)-el.offsetWidth-pad;
  const maxY=(parent?.clientHeight||window.innerHeight)-el.offsetHeight-pad;
  return{
    x:Math.max(pad,Math.min(Math.max(pad,maxX),x)),
    y:Math.max(pad,Math.min(Math.max(pad,maxY),y))
  };
}
function applyThemeColors(){
  const c=State.themeColors;
  const targets=[document.documentElement?.style,document.body?.style].filter(Boolean);
  const setVar=(name,value)=>targets.forEach(t=>t.setProperty(name,value));
  const panelBorder=c.panelBorder||c.panelA;
  const accent=c.accent||"#0ea5e9";
  const accentText=State.themeMode==="light"?"#0b1120":c.text;
  const accentShadow="rgba(14,165,233,0.45)";
  setVar("--bg-main","linear-gradient(135deg,"+c.bgA+","+c.bgB+")");
  setVar("--header-bg","linear-gradient(135deg,"+c.headerA+","+c.headerB+")");
  setVar("--panel-bg","linear-gradient(135deg,"+c.panelA+","+c.panelB+")");
  setVar("--panel-border",panelBorder);
  setVar("--text-main",c.text);
  setVar("--btn-bg",c.panelA);
  setVar("--btn-border",panelBorder);
  setVar("--btn-bg-active",accent);
  setVar("--btn-text",c.text);
  setVar("--btn-text-active",accentText);
  setVar("--toggle-bg",c.panelA);
  setVar("--toggle-border",panelBorder);
  setVar("--toggle-text",c.text);
  setVar("--toggle-active-bg",accent);
  setVar("--toggle-active-border",accent);
  setVar("--toggle-active-text",accentText);
  setVar("--toggle-active-shadow",accentShadow);
  setVar("--input-bg",c.panelA);
  setVar("--input-border",panelBorder);
}
function showView(name){
  Views.login.classList.add("hidden");
  Views.config.classList.add("hidden");
  Views.main.classList.add("hidden");
  if(name==="login")Views.login.classList.remove("hidden");
  if(name==="config")Views.config.classList.remove("hidden");
  if(name==="main")Views.main.classList.remove("hidden");
}
populateDbVersions();
Config.dbType.addEventListener("change",()=>{
  populateDbVersions();
  State.dbVersion=Config.dbVersion.value||DbVersions[Config.dbType.value]||"latest";
});
applyThemeColors();
loadThemeControls();
setDisplayMode(State.displayMode||"en",{silent:true});
setThemeMode(State.themeMode||"dark");
syncThemeToggleUI();
function setDisplayMode(mode,{silent}={}){
  const next=mode==="fa"?"fa":"en";
  State.displayMode=next;
  const isEn=next==="en";
  Header.displayEnBtn?.classList.toggle("toggle-active",isEn);
  Header.displayFaBtn?.classList.toggle("toggle-active",!isEn);
  Header.displayEnBtn?.setAttribute("aria-pressed",isEn?"true":"false");
  Header.displayFaBtn?.setAttribute("aria-pressed",!isEn?"true":"false");
  if(!silent)updateAllUI();
}
function initSidePanelDrag(){
  const header=document.querySelector(".side-panel-header");
  if(!PanelUI.side||!header)return;
  let drag=null;
  header.addEventListener("mousedown",e=>{
    if(e.button!==0)return;
    const rect=PanelUI.side.getBoundingClientRect();
    drag={
      offsetX:e.clientX-rect.left,
      offsetY:e.clientY-rect.top,
      parentRect:(PanelUI.side.offsetParent||document.body).getBoundingClientRect()
    };
    PanelUI.side.style.right="";
    PanelUI.side.style.bottom="";
    document.addEventListener("mousemove",onDrag);
    document.addEventListener("mouseup",stopDrag);
  });
  function onDrag(e){
    if(!drag)return;
    const parentRect=drag.parentRect||(PanelUI.side.offsetParent||document.body).getBoundingClientRect();
    const nextX=e.clientX-drag.offsetX-parentRect.left;
    const nextY=e.clientY-drag.offsetY-parentRect.top;
    const clamped=clampWithinParent(PanelUI.side,nextX,nextY,0);
    PanelUI.side.style.left=clamped.x+"px";
    PanelUI.side.style.top=clamped.y+"px";
  }
  function stopDrag(){
    drag=null;
    document.removeEventListener("mousemove",onDrag);
    document.removeEventListener("mouseup",stopDrag);
  }
}
function setHeaderPinned(pinned){
  if(!Header.container||!Header.pinBtn)return;
  const isPinned=!!pinned;
  Header.container.classList.toggle("header-hidden",!isPinned);
  Header.pinBtn.classList.toggle("active",isPinned);
  Header.pinBtn.setAttribute("aria-pressed",isPinned?"true":"false");
  if(Views.main){
    Views.main.classList.toggle("header-unpinned",!isPinned);
  }
}
setHeaderPinned(true);
if(Header.pinBtn){
  Header.pinBtn.addEventListener("click",()=>{
    const currentlyPinned=!Header.container.classList.contains("header-hidden");
    setHeaderPinned(!currentlyPinned);
  });
}
function loginSubmit(e){
  if(e&&e.preventDefault)e.preventDefault();
  const u=Login.user.value.trim();
  const p=Login.pass.value.trim();
  if(u==="admin"&&p==="admin"){
    Login.error.textContent="";
    showView("config");
  }else{
    Login.error.textContent="Invalid username or password.";
  }
}
function safeLogin(e){
  try{
    loginSubmit(e);
  }catch(err){
    console.error(err);
    if(Login.error)Login.error.textContent="خطای غیرمنتظره: "+(err.message||err);
  }
}
Login.btn.addEventListener("click",e=>safeLogin(e));
Login.pass.addEventListener("keydown",e=>{if(e.key==="Enter")safeLogin(e)});
Config.back.addEventListener("click",()=>showView("login"));
Config.cont.addEventListener("click",()=>{
  Config.status.textContent="Preparing designer...";
  State.dbName=Config.dbName.value.trim()||"my_database";
  State.dbType=Config.dbType.value;
  State.dbVersion=Config.dbVersion.value||DbVersions[State.dbType]||"latest";
  const jf=Config.jsonFile.files[0];
  const sf=Config.sqlFile.files[0];
  if(jf){
    const r=new FileReader();
    r.onload=()=>{try{importJsonSchema(r.result)}catch(e){} finalizeMain()};
    r.readAsText(jf)
  }else if(sf){
    const r=new FileReader();
    r.onload=()=>{importSqlText(r.result);finalizeMain()};
    r.readAsText(sf)
  }else finaliseEmpty()
});
function finaliseEmpty(){
  if(!State.schemas.length){State.schemas.push(defaultSchema())}
  if(!State.tables.length){
    const t=createTable("table_1",State.schemas[0].id,120,120);
    State.tables.push(t)
  }
  State.selectedTableIds=new Set([State.tables[0].id]);
  finalizeMain()
}
function finalizeMain(){
  setDbMeta();
  syncSchemaSelects();
  updateFieldTypeOptions();
  updateAllUI();
  initOptionPanel();
  showView("main");
  Config.status.textContent=""
}
function setDbMeta(){
  Header.dbName.textContent=State.dbName;
  let t="MySQL";
  if(State.dbType==="postgres")t="PostgreSQL";
  if(State.dbType==="sqlite")t="SQLite";
  if(State.dbType==="sqlserver")t="SQL Server";
  Header.dbType.textContent=t;
  Header.dbVersion.textContent=State.dbVersion==="latest"?DbVersions[State.dbType]||"latest":State.dbVersion;
  if(Header.subtitle)Header.subtitle.textContent=t+" - "+State.dbName
}
Controls.logoutBtn.addEventListener("click",()=>{
  State.tables=[];
  State.relations=[];
  State.schemas=[];
  State.selectedTableIds=new Set();
  State.selectedFieldId=null;
  State.selectedRelationId=null;
  State.zoom=1;
  State.offsetX=80;
  State.offsetY=80;
  LeftPanel.jsonArea.value="";
  LeftPanel.sqlArea.value="";
  Login.user.value="";
  Login.pass.value="";
  Canvas.inner.innerHTML="";
  Canvas.relationLayer.innerHTML="";
  showView("login")
});
Header.displayEnBtn.addEventListener("click",()=>{
  setDisplayMode("en");
});
Header.displayFaBtn.addEventListener("click",()=>{
  setDisplayMode("fa");
});
function setThemeMode(mode){
  const next=mode==="light"?"light":"dark";
  const isLight=next==="light";
  State.themeMode=next;
  if(isLight){
    State.themeColors={bgA:"#f4f7fb",bgB:"#e8edf4",headerA:"#f8fafc",headerB:"#e2e8f0",panelA:"#ffffff",panelB:"#f1f5f9",text:"#0f172a",panelBorder:"#cbd5e1"};
  }else{
    State.themeColors={bgA:"#010409",bgB:"#0b1220",headerA:"#020617",headerB:"#0b1327",panelA:"#0a0f1d",panelB:"#0f172a",text:"#e5e7eb",panelBorder:"#0d1117"};
  }
  applyThemeColors();
  loadThemeControls();
  syncThemeToggleUI();
  updateAllUI();
  document.body.classList.toggle("theme-light",isLight);
  document.body.classList.toggle("theme-dark",!isLight);
}
function syncThemeToggleUI(){
  const isLight=State.themeMode==="light";
  const darkBtn=Appearance.themeDarkBtn;
  const lightBtn=Appearance.themeLightBtn;
  if(darkBtn&&lightBtn){
    darkBtn.classList.remove("toggle-active","toggle-inactive");
    lightBtn.classList.remove("toggle-active","toggle-inactive");
    darkBtn.classList.toggle("toggle-active",!isLight);
    lightBtn.classList.toggle("toggle-active",isLight);
    darkBtn.classList.toggle("toggle-inactive",isLight);
    lightBtn.classList.toggle("toggle-inactive",!isLight);
    darkBtn.setAttribute("aria-pressed",!isLight?"true":"false");
    lightBtn.setAttribute("aria-pressed",isLight?"true":"false");
  }
}
function loadThemeControls(){
  if(!Appearance.colorBgA)return;
  const c=State.themeColors;
  Appearance.colorBgA.value=c.bgA;
  Appearance.colorBgB.value=c.bgB;
  Appearance.colorHeaderA.value=c.headerA;
  Appearance.colorHeaderB.value=c.headerB;
  Appearance.colorPanelA.value=c.panelA;
  Appearance.colorPanelB.value=c.panelB;
  Appearance.colorText.value=c.text;
}
function applyCustomThemeFromControls(){
  State.themeColors={
    bgA:Appearance.colorBgA.value||State.themeColors.bgA,
    bgB:Appearance.colorBgB.value||State.themeColors.bgB,
    headerA:Appearance.colorHeaderA.value||State.themeColors.headerA,
    headerB:Appearance.colorHeaderB.value||State.themeColors.headerB,
    panelA:Appearance.colorPanelA.value||State.themeColors.panelA,
    panelB:Appearance.colorPanelB.value||State.themeColors.panelB,
    text:Appearance.colorText.value||State.themeColors.text,
    panelBorder:Appearance.colorPanelA.value||State.themeColors.panelA
  };
  applyThemeColors();
  updateAllUI();
}
if(Appearance.applyBtn)Appearance.applyBtn.addEventListener("click",applyCustomThemeFromControls);
if(Appearance.resetBtn)Appearance.resetBtn.addEventListener("click",()=>setThemeMode(State.themeMode));
if(Appearance.themeDarkBtn)Appearance.themeDarkBtn.addEventListener("click",()=>setThemeMode("dark"));
if(Appearance.themeLightBtn)Appearance.themeLightBtn.addEventListener("click",()=>setThemeMode("light"));
if(Appearance.autoSortBtn)Appearance.autoSortBtn.addEventListener("click",autoSortTables);
if(Appearance.editDbNameBtn)Appearance.editDbNameBtn.addEventListener("click",()=>{
  const name=prompt("Database name",State.dbName);
  if(!name)return;
  State.dbName=name.trim()||State.dbName;
  setDbMeta();
  updateJsonArea();
});
function zoomStep(){
  return State.zoom<0.1?0.01:0.1
}
function bumpZoom(dir){
  const step=zoomStep();
  const delta=dir==="in"?step:-step;
  setZoom(State.zoom+delta)
}
if(Canvas.zoomInBtn)Canvas.zoomInBtn.addEventListener("click",()=>bumpZoom("in"));
if(Canvas.zoomOutBtn)Canvas.zoomOutBtn.addEventListener("click",()=>bumpZoom("out"));
if(Canvas.zoomResetBtn)Canvas.zoomResetBtn.addEventListener("click",()=>setZoom(1));
if(OptionUI.zoomInBtn)OptionUI.zoomInBtn.addEventListener("click",()=>bumpZoom("in"));
if(OptionUI.zoomOutBtn)OptionUI.zoomOutBtn.addEventListener("click",()=>bumpZoom("out"));
if(OptionUI.zoomResetBtn)OptionUI.zoomResetBtn.addEventListener("click",()=>setZoom(1));
function setMode(mode){
  State.interactionMode=mode;
  const bindings=[
    {btn:Canvas.modeSelectBtn,mode:"select"},
    {btn:Canvas.modeMoveBtn,mode:"move"},
    {btn:Canvas.modePanBtn,mode:"pan"},
    {btn:OptionUI.selectBtn,mode:"select"},
    {btn:OptionUI.moveBtn,mode:"move"},
    {btn:OptionUI.panBtn,mode:"pan"}
  ];
  bindings.forEach(entry=>{
    if(entry.btn)entry.btn.classList.toggle("active",mode===entry.mode)
  });
  State.panMode=mode==="pan";
  if(Canvas.panModeBtn){
    Canvas.panModeBtn.classList.toggle("active",State.panMode);
    Canvas.panModeBtn.classList.toggle("bg-sky-600",State.panMode);
    Canvas.panModeBtn.classList.toggle("border-sky-500",State.panMode);
  }
  Canvas.pan.style.cursor=mode==="pan"?"grab":"default";
}
if(Canvas.modeSelectBtn)Canvas.modeSelectBtn.addEventListener("click",()=>setMode("select"));
if(Canvas.modeMoveBtn)Canvas.modeMoveBtn.addEventListener("click",()=>setMode("move"));
if(Canvas.modePanBtn)Canvas.modePanBtn.addEventListener("click",()=>setMode("pan"));
if(OptionUI.selectBtn)OptionUI.selectBtn.addEventListener("click",()=>setMode("select"));
if(OptionUI.moveBtn)OptionUI.moveBtn.addEventListener("click",()=>setMode("move"));
if(OptionUI.panBtn)OptionUI.panBtn.addEventListener("click",()=>setMode("pan"));
if(Canvas.panModeBtn)Canvas.panModeBtn.addEventListener("click",()=>{
  const next=!State.panMode;
  if(next)setMode("pan"); else if(State.interactionMode==="pan")setMode("move");
  State.panMode=next;
});
Canvas.zoomIndicator.textContent="100%";
Canvas.statusZoom.textContent="100%";

function sortedSections(){
  return PanelUI.sections.slice().sort((a,b)=>{
    const ap=a.element.dataset.pinned==="true"?1:0;
    const bp=b.element.dataset.pinned==="true"?1:0;
    if(ap!==bp)return bp-ap;
    return Number(a.element.dataset.order||0)-Number(b.element.dataset.order||0)
  })
}
function floatingSections(){
  return sortedSections().filter(sec=>sec.element.dataset.pinned!=="true")
}
function syncSectionLayout(){
  const ordered=sortedSections();
  ordered.forEach((sec,i)=>{
    sec.element.dataset.order=i;
    PanelUI.scroll?.appendChild(sec.element);
    const pinned=sec.element.dataset.pinned==="true";
    if(sec.pinBtn){
      sec.pinBtn.classList.toggle("active",pinned);
      sec.pinBtn.setAttribute("aria-pressed",pinned?"true":"false")
    }
  })
}
function toggleSectionPin(sec){
  if(!sec)return;
  const pinned=sec.element.dataset.pinned==="true";
  if(pinned){
    sec.element.dataset.pinned="false"
  }else{
    PanelUI.sections.forEach(s=>{s.element.dataset.pinned="false"});
    sec.element.dataset.pinned="true";
    sec.element.dataset.order=0
  }
  syncSectionLayout()
}
function reorderFloating(sec,targetIdx){
  const ordered=sortedSections();
  const pinnedList=ordered.filter(s=>s.element.dataset.pinned==="true");
  const floats=ordered.filter(s=>s.element.dataset.pinned!=="true");
  const cur=floats.indexOf(sec);
  if(cur===-1)return;
  const safeIdx=Math.max(0,Math.min(floats.length-1,targetIdx));
  floats.splice(cur,1);
  floats.splice(safeIdx,0,sec);
  const merged=[...pinnedList,...floats];
  merged.forEach((s,i)=>{s.element.dataset.order=i});
  syncSectionLayout()
}
let sectionDragState=null;
function startSectionDrag(sec,e){
  if(sec.element.dataset.pinned==="true")return;
  sectionDragState={sec};
  sec.element.classList.add("dragging");
  document.addEventListener("mousemove",onSectionDragMove);
  document.addEventListener("mouseup",endSectionDrag)
}
function onSectionDragMove(e){
  if(!sectionDragState)return;
  const floats=floatingSections();
  const curIdx=floats.indexOf(sectionDragState.sec);
  if(curIdx===-1)return;
  let targetIdx=floats.length-1;
  for(let i=0;i<floats.length;i++){
    const rect=floats[i].element.getBoundingClientRect();
    const mid=rect.top+rect.height/2;
    if(e.clientY<mid){targetIdx=i;break}
  }
  if(targetIdx!==curIdx)reorderFloating(sectionDragState.sec,targetIdx)
}
function endSectionDrag(){
  if(!sectionDragState)return;
  sectionDragState.sec.element.classList.remove("dragging");
  document.removeEventListener("mousemove",onSectionDragMove);
  document.removeEventListener("mouseup",endSectionDrag);
  sectionDragState=null
}
try{
  PanelUI.sections.forEach(sec=>{
    if(sec.toggle&&sec.toggle.dataset.target){
      sec.body=document.getElementById(sec.toggle.dataset.target)
    }
    if(sec.pinBtn)sec.pinBtn.addEventListener("click",()=>toggleSectionPin(sec));
    if(sec.handle){
      sec.handle.addEventListener("contextmenu",e=>e.preventDefault());
      sec.handle.addEventListener("mousedown",e=>{if(e.button!==0&&e.button!==2)return;startSectionDrag(sec,e)})
    }
  });
  syncSectionLayout();
  setPanelVisible(true);
  initSidePanelDrag();
}catch(err){
  console.error("Side panel init failed",err);
}

function setPanelVisible(show){
  if(!PanelUI.side)return;
  PanelUI.side.classList.toggle("hidden",!show);
  if(PanelUI.toggleBtn){
    PanelUI.toggleBtn.classList.toggle("active",show);
    PanelUI.toggleBtn.classList.toggle("toggle-active",show);
    PanelUI.toggleBtn.setAttribute("aria-pressed",show?"true":"false");
  }
}
if(PanelUI.toggleBtn&&PanelUI.side){
  PanelUI.toggleBtn.addEventListener("click",()=>{
    const hidden=PanelUI.side.classList.toggle("hidden");
    PanelUI.toggleBtn.classList.toggle("active",!hidden);
    PanelUI.toggleBtn.classList.toggle("toggle-active",!hidden);
    PanelUI.toggleBtn.setAttribute("aria-pressed",!hidden?"true":"false");
  });
  PanelUI.toggleBtn.classList.add("panel-toggle-header");
}
function setOptionPanelVisible(show){
  if(!OptionUI.panel)return;
  OptionUI.panel.classList.toggle("hidden",!show);
  if(OptionUI.toggleBtn){
    OptionUI.toggleBtn.classList.toggle("active",show);
    OptionUI.toggleBtn.classList.toggle("toggle-active",show);
    OptionUI.toggleBtn.setAttribute("aria-pressed",show?"true":"false");
  }
}
if(OptionUI.toggleBtn&&OptionUI.panel){
  OptionUI.toggleBtn.addEventListener("click",()=>{
    const hidden=OptionUI.panel.classList.toggle("hidden");
    OptionUI.toggleBtn.classList.toggle("active",!hidden);
    OptionUI.toggleBtn.classList.toggle("toggle-active",!hidden);
    OptionUI.toggleBtn.setAttribute("aria-pressed",!hidden?"true":"false");
  });
}
function initOptionPanel(){
  if(optionPanelInitialized)return;
  optionPanelInitialized=true;
  try{
    setOptionPanelVisible(true);
    initOptionPanelDrag();
  }catch(err){
    console.error("Option panel init failed",err);
  }
}
function initOptionPanelDrag(){
  if(!OptionUI.panel||!OptionUI.handle)return;
  let drag=null;
  OptionUI.panel.style.right="";
  OptionUI.panel.style.bottom="";
  const parent=OptionUI.panel.offsetParent||document.body;
  const startX=12;
  const startY=Math.max(0,((parent.clientHeight||window.innerHeight)-OptionUI.panel.offsetHeight)/2);
  const initial=clampWithinParent(OptionUI.panel,startX,startY,0);
  OptionUI.panel.style.left=initial.x+"px";
  OptionUI.panel.style.top=initial.y+"px";
  OptionUI.panel.style.transform="";
  function startDrag(e){
    OptionUI.panel.style.transform="";
    drag={
      startX:e.clientX,
      startY:e.clientY,
      origX:OptionUI.panel.offsetLeft,
      origY:OptionUI.panel.offsetTop
    };
    document.addEventListener("mousemove",onDrag);
    document.addEventListener("mouseup",endDrag);
  }
  OptionUI.handle.addEventListener("mousedown",e=>startDrag(e));
  OptionUI.panel.addEventListener("mousedown",e=>{
    if(e.target.closest("button"))return;
    startDrag(e);
  });
  function onDrag(e){
    if(!drag)return;
    const dx=e.clientX-drag.startX;
    const dy=e.clientY-drag.startY;
    let nextX=drag.origX+dx;
    let nextY=drag.origY+dy;
    const clamped=clampWithinParent(OptionUI.panel,nextX,nextY,0);
    OptionUI.panel.style.left=clamped.x+"px";
    OptionUI.panel.style.top=clamped.y+"px";
  }
  function endDrag(){
    drag=null;
    document.removeEventListener("mousemove",onDrag);
    document.removeEventListener("mouseup",endDrag);
  }
}
PanelUI.accordions.forEach(btn=>{
  const targetId=btn.dataset.target;
  const body=document.getElementById(targetId);
  if(!body)return;
  btn.addEventListener("click",()=>{
    const collapsed=body.classList.toggle("collapsed");
    btn.classList.toggle("collapsed",collapsed);
  })
});
if(Canvas.pan)Canvas.pan.addEventListener("wheel",e=>{
  if(e.ctrlKey||e.metaKey){
    e.preventDefault();
    const delta=e.deltaY<0?zoomStep():-zoomStep();
    setZoom(State.zoom+delta)
  }
},{passive:false});
function setZoom(z){
  if(z<0.01)z=0.01;
  if(z>10)z=10;
  State.zoom=z;
  applyTransform();
  const label=Math.round(z*100)+"%";
  Canvas.zoomIndicator.textContent=label;
  Canvas.statusZoom.textContent=label
}
let panDrag=null;
if(Canvas.pan)Canvas.pan.addEventListener("mousedown",e=>{
  const target=e.target;
  if(target.closest(".table-node")||target.closest("svg"))return;
  if(State.interactionMode==="pan"||State.panMode){
    panDrag={startX:e.clientX,startY:e.clientY,origX:State.offsetX,origY:State.offsetY};
    Canvas.pan.style.cursor="grabbing";
    document.addEventListener("mousemove",onPanMove);
    document.addEventListener("mouseup",onPanEnd)
  }else{
    State.selectedTableIds=new Set();
    State.selectedFieldId=null;
    State.selectedRelationId=null;
    updateAllUI()
  }
});
function onPanMove(e){
  if(!panDrag)return;
  const dx=(e.clientX-panDrag.startX)/State.zoom;
  const dy=(e.clientY-panDrag.startY)/State.zoom;
  State.offsetX=panDrag.origX+dx;
  State.offsetY=panDrag.origY+dy;
  applyTransform()
}
function onPanEnd(){
  panDrag=null;
  Canvas.pan.style.cursor=State.panMode?"grab":"default";
  document.removeEventListener("mousemove",onPanMove);
  document.removeEventListener("mouseup",onPanEnd)
}
function applyTransform(){
  Canvas.inner.style.transform="translate("+State.offsetX+"px,"+State.offsetY+"px) scale("+State.zoom+")";
  Canvas.relationLayer.style.transform="translate("+State.offsetX+"px,"+State.offsetY+"px) scale("+State.zoom+")";
  Canvas.relationLayer.style.transformOrigin="0 0"
}
function autoSortTables(){
  const gapX=260;
  const gapY=220;
  State.tables.forEach((t,idx)=>{
    const row=Math.floor(idx/3);
    const col=idx%3;
    t.x=120+col*gapX;
    t.y=120+row*gapY;
  });
  updateAllUI();
}
function defaultSchema(){
  return{id:uuid(),name:"public",faName:"عمومی",colorA:"#38bdf8",colorB:"#8b5cf6"}
}
function ensureSchemas(){
  if(!State.schemas.length)State.schemas.push(defaultSchema())
}
function createTable(name,schemaId,x,y){
  return{
    id:uuid(),
    name:name||"table",
    faName:"",
    description:"",
    schemaId:schemaId||State.schemas[0]?.id||null,
    deleteBehavior:"none",
    x:x||100,
    y:y||100,
    fields:[]
  }
}
function createField(){
  const opts=DataTypes[State.dbType]||["TEXT"];
  return{
    id:uuid(),
    name:"field",
    faName:"",
    type:opts[0],
    nullable:true,
    primary:false,
    description:"",
    pinned:false
  }
}
function createRelation(fromTableId,fromFieldId,toTableId,toFieldId,onDelete){
  return{
    id:uuid(),
    fromTableId,
    fromFieldId,
    toTableId,
    toFieldId,
    onDelete:onDelete||"no_action",
    style:"schema",
    colorA:"#38bdf8",
    colorB:"#8b5cf6",
    sourceLabel:"",
    centerLabel:"",
    targetLabel:"",
    bendOffset:0,
    ctrlX:null,
    ctrlY:null
  }
}
function getTableById(id){return State.tables.find(t=>t.id===id)||null}
function getFieldById(tid,fid){const t=getTableById(tid);if(!t)return null;return t.fields.find(f=>f.id===fid)||null}
function getSchemaById(id){return State.schemas.find(s=>s.id===id)||null}
function schemaGradient(schema){
  if(!schema)return"linear-gradient(135deg,#0f172a,#1f2937)";
  return"linear-gradient(135deg,"+schema.colorA+","+schema.colorB+")"
}
function schemaMainColor(schema){
  if(!schema)return"#1f2937";
  return schema.colorA||"#38bdf8"
}
function updateAllUI(){
  ensureSchemas();
  renderCanvas();
  updateSelectionStatus();
  updateTableSelect();
  updateTableEditor();
  updateFieldEditor();
  updateRelationsEditor();
  updateSchemaManager();
  updateJsonArea();
  updateSqlArea()
}
function renderCanvas(){
  Canvas.inner.innerHTML="";
  Canvas.relationLayer.innerHTML="";
  applyTransform();
  renderRelations();
  State.tables.forEach(t=>renderTableNode(t))
}
function renderTableNode(t){
  const isFa=State.displayMode==="fa";
  const schema=getSchemaById(t.schemaId);
  const node=document.createElement("div");
  node.className="table-node border";
  node.style.left=t.x+"px";
  node.style.top=t.y+"px";
  const baseWidth=t.tableWidth||260;
  const autoHeight=90+Math.max(t.fields.length*24,60);
  const nodeHeight=Math.max(t.tableHeight||0,autoHeight);
  node.style.width=baseWidth+"px";
  node.style.height=nodeHeight+"px";
  const selected=State.selectedTableIds.has(t.id);
  const borderColor=selected?"#fb923c":schemaMainColor(schema);
  node.style.borderColor=borderColor;
  node.style.boxShadow=selected?"0 0 0 1px #fb923c,0 18px 40px rgba(0,0,0,0.95)":"0 18px 40px rgba(0,0,0,0.85)";
  const header=document.createElement("div");
  header.className="table-header";
  const title=document.createElement("div");
  title.className="table-title";
  title.textContent=isFa&&(t.faName||"")?t.faName:t.name;
  const cap=document.createElement("div");
  cap.className="table-caption";
  cap.textContent=schema?(isFa&&(schema.faName||"")?schema.faName:schema.name):"no schema";
  cap.style.backgroundImage=schemaGradient(schema);
  cap.style.borderColor="#0f172a";
  header.appendChild(title);
  header.appendChild(cap);
  const desc=document.createElement("div");
  desc.className="table-desc";
  desc.textContent=t.description||"";
  const fieldsWrap=document.createElement("div");
  fieldsWrap.className="table-fields scrollbar-thin";
  fieldsWrap.style.maxHeight="none";
  const fields=[...t.fields].sort((a,b)=>Number(b.pinned)-Number(a.pinned));
  fields.forEach(f=>{
    const row=document.createElement("div");
    row.className="table-field-row"+(State.selectedFieldId===f.id?" selected":"");
    const left=document.createElement("div");
    left.className="flex items-center gap-1";
    if(f.pinned){
      const pin=document.createElement("span");
      pin.textContent="پین";
      pin.className="text-[10px]";
      left.appendChild(pin);
    }
    const name=document.createElement("div");
    name.className="table-field-name";
    name.textContent=isFa&&(f.faName||"")?f.faName:f.name;
    const type=document.createElement("div");
    type.className="table-field-type";
    type.textContent=f.type;
    left.appendChild(name);
    left.appendChild(type);
    const right=document.createElement("div");
    right.className="table-field-flags";
    const flags=[];
    if(f.primary)flags.push("PK");
    if(!f.nullable)flags.push("NN");
    right.textContent=flags.join(" ");
    row.appendChild(left);
    row.appendChild(right);
    row.addEventListener("mousedown",e=>{
      e.stopPropagation();
      if(e.ctrlKey||e.metaKey){
        if(State.selectedTableIds.has(t.id))State.selectedTableIds.delete(t.id);else State.selectedTableIds.add(t.id)
      }else{
        State.selectedTableIds=new Set([t.id])
      }
      State.selectedFieldId=f.id;
      State.selectedRelationId=null;
      updateAllUI()
    });
    fieldsWrap.appendChild(row)
  });
  node.appendChild(header);
  if(t.description)node.appendChild(desc);
  node.appendChild(fieldsWrap);
  const resize=document.createElement("div");
  resize.className="table-resize";
  resize.addEventListener("mousedown",e=>startResizeTable(t,e));
  node.appendChild(resize);
  node.addEventListener("mousedown",e=>{
    if(e.target.closest(".table-field-row"))return;
    e.preventDefault();
    if(e.ctrlKey||e.metaKey){
      if(State.selectedTableIds.has(t.id))State.selectedTableIds.delete(t.id);else State.selectedTableIds.add(t.id)
    }else{
      State.selectedTableIds=new Set([t.id])
    }
    State.selectedFieldId=null;
    State.selectedRelationId=null;
    startDragTable(t,e);
    updateAllUI()
  });
  Canvas.inner.appendChild(node)
}
function getTableSize(t){
  const baseWidth=t.tableWidth||260;
  const autoHeight=90+Math.max(t.fields.length*24,60);
  const height=Math.max(t.tableHeight||0,autoHeight);
  return{width:baseWidth,height};
}
function clampTableToView(t){
  if(!Canvas.pan)return;
  const rect=Canvas.pan.getBoundingClientRect();
  const pad=0;
  const size=getTableSize(t);
  const minX=(pad/State.zoom)-State.offsetX;
  const minY=(pad/State.zoom)-State.offsetY;
  const maxX=(rect.width-pad)/State.zoom-State.offsetX-size.width;
  const maxY=(rect.height-pad)/State.zoom-State.offsetY-size.height;
  t.x=Math.min(Math.max(t.x,minX),Math.max(minX,maxX));
  t.y=Math.min(Math.max(t.y,minY),Math.max(minY,maxY));
}
let dragTableState=null;
let resizeState=null;
function startDragTable(t,e){
  if(State.interactionMode!=="move")return;
  dragTableState={id:t.id,startX:e.clientX,startY:e.clientY,origX:t.x,origY:t.y};
  document.addEventListener("mousemove",onTableDragMove);
  document.addEventListener("mouseup",onTableDragEnd)
}
function startResizeTable(t,e){
  e.stopPropagation();
  resizeState={id:t.id,startX:e.clientX,startY:e.clientY,origW:t.tableWidth||260,origH:t.tableHeight||0};
  document.addEventListener("mousemove",onTableResizeMove);
  document.addEventListener("mouseup",endTableResize);
}
function onTableDragMove(e){
  if(!dragTableState)return;
  const t=getTableById(dragTableState.id);
  if(!t)return;
  const dx=(e.clientX-dragTableState.startX)/State.zoom;
  const dy=(e.clientY-dragTableState.startY)/State.zoom;
  t.x=dragTableState.origX+dx;
  t.y=dragTableState.origY+dy;
  clampTableToView(t);
  renderCanvas()
}
function onTableDragEnd(){
  dragTableState=null;
  document.removeEventListener("mousemove",onTableDragMove);
  document.removeEventListener("mouseup",onTableDragEnd)
}
function onTableResizeMove(e){
  if(!resizeState)return;
  const t=getTableById(resizeState.id);
  if(!t)return;
  const dx=(e.clientX-resizeState.startX)/State.zoom;
  const dy=(e.clientY-resizeState.startY)/State.zoom;
  t.tableWidth=Math.max(200,resizeState.origW+dx);
  t.tableHeight=Math.max(160,resizeState.origH+dy);
  renderCanvas();
}
function endTableResize(){
  resizeState=null;
  document.removeEventListener("mousemove",onTableResizeMove);
  document.removeEventListener("mouseup",endTableResize);
}
function renderRelations(){
  const layer=Canvas.relationLayer;
  layer.innerHTML="";
  const defs=document.createElementNS("http://www.w3.org/2000/svg","defs");
  const marker=document.createElementNS("http://www.w3.org/2000/svg","marker");
  marker.setAttribute("id","arrow-head");
  marker.setAttribute("markerWidth","6");
  marker.setAttribute("markerHeight","6");
  marker.setAttribute("refX","5");
  marker.setAttribute("refY","3");
  marker.setAttribute("orient","auto");
  const markerPath=document.createElementNS("http://www.w3.org/2000/svg","path");
  markerPath.setAttribute("d","M0,0 L6,3 L0,6 Z");
  markerPath.setAttribute("fill","#38bdf8");
  marker.appendChild(markerPath);
  defs.appendChild(marker);
  layer.appendChild(defs);
  State.relations.forEach(rel=>{
    const ft=getTableById(rel.fromTableId);
    const tt=getTableById(rel.toTableId);
    const ff=getFieldById(rel.fromTableId,rel.fromFieldId)||ft?.fields[0];
    const tf=getFieldById(rel.toTableId,rel.toFieldId)||tt?.fields[0];
    if(!ft||!tt||!ff||!tf)return;
    const fi=ft.fields.indexOf(ff);
    const ti=tt.fields.indexOf(tf);
    const fromX=ft.x+(ft.schemaId===tt.schemaId?ft.id>tt.id?0:ft.tableWidth||190:ft.tableWidth||190);
    const fromY=ft.y+30+fi*16;
    const toX=tt.x+(ft.schemaId===tt.schemaId?tt.id>ft.id?0:tt.tableWidth||190:0);
    const toY=tt.y+30+ti*16;
    const bend=typeof rel.bendOffset==="number"?rel.bendOffset:0;
    const defCtrlX=(fromX+toX)/2 + bend;
    const defCtrlY=(fromY+toY)/2;
    const ctrlX=typeof rel.ctrlX==="number"?rel.ctrlX:defCtrlX;
    const ctrlY=typeof rel.ctrlY==="number"?rel.ctrlY:defCtrlY;
    const pathData=`M ${fromX} ${fromY} L ${ctrlX} ${fromY} L ${ctrlX} ${toY} L ${toX} ${toY}`;
    const gradientId="grad-"+rel.id;
    let strokeColor="#38bdf8";
    let strokeDash="";
    if(rel.style==="schema"){
      const schema=getSchemaById(ft.schemaId);
      strokeColor=schemaMainColor(schema)
    }else if(rel.style==="solid"){
      strokeColor=rel.colorA
    }else if(rel.style==="dashed"){
      strokeColor=rel.colorA;
      strokeDash="6 4"
    }else if(rel.style==="dotted"){
      strokeColor=rel.colorA;
      strokeDash="2 4"
    }else if(rel.style==="gradient"){
      const lg=document.createElementNS("http://www.w3.org/2000/svg","linearGradient");
      lg.setAttribute("id",gradientId);
      lg.setAttribute("x1","0%");
      lg.setAttribute("y1","0%");
      lg.setAttribute("x2","100%");
      lg.setAttribute("y2","0%");
      const s1=document.createElementNS("http://www.w3.org/2000/svg","stop");
      s1.setAttribute("offset","0%");
      s1.setAttribute("stop-color",rel.colorA);
      const s2=document.createElementNS("http://www.w3.org/2000/svg","stop");
      s2.setAttribute("offset","100%");
      s2.setAttribute("stop-color",rel.colorB);
      lg.appendChild(s1);lg.appendChild(s2);
      defs.appendChild(lg)
    }
    const path=document.createElementNS("http://www.w3.org/2000/svg","path");
    path.setAttribute("d",pathData);
    path.setAttribute("class","line-path");
    path.setAttribute("stroke-width",State.selectedRelationId===rel.id?2.6:2);
    if(rel.style==="gradient")path.setAttribute("stroke","url(#"+gradientId+")");else path.setAttribute("stroke",strokeColor);
    if(strokeDash)path.setAttribute("stroke-dasharray",strokeDash);
    if(State.selectedRelationId===rel.id)path.setAttribute("stroke","#fb923c");
    path.setAttribute("marker-end","url(#arrow-head)");
    layer.appendChild(path);
    const hit=document.createElementNS("http://www.w3.org/2000/svg","path");
    hit.setAttribute("d",pathData);
    hit.setAttribute("class","relation-hit");
    hit.addEventListener("mousedown",e=>{
      e.stopPropagation();
      if(e.ctrlKey||e.metaKey)State.selectedRelationId=State.selectedRelationId===rel.id?null:rel.id;else State.selectedRelationId=rel.id;
      State.selectedTableIds=new Set();
      State.selectedFieldId=null;
      updateAllUI()
    });
    layer.appendChild(hit);
    const centerLabel=rel.centerLabel;
    const srcLabel=rel.sourceLabel;
    const tgtLabel=rel.targetLabel;
    const midY=(fromY+toY)/2;
    const dirLabel= (ft.name||"") +" ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ "+ (tt.name||"");
    const dirText=document.createElementNS("http://www.w3.org/2000/svg","text");
    dirText.setAttribute("x",midX);
    dirText.setAttribute("y",midY-12);
    dirText.setAttribute("text-anchor","middle");
    dirText.setAttribute("font-size","9");
    dirText.setAttribute("fill","#94a3b8");
    dirText.textContent=dirLabel;
    layer.appendChild(dirText);
    if(centerLabel){
      const label=document.createElementNS("http://www.w3.org/2000/svg","text");
      label.setAttribute("x",midX);
      label.setAttribute("y",midY-4);
      label.setAttribute("text-anchor","middle");
      label.setAttribute("font-size","9");
      label.setAttribute("fill","#e5e7eb");
      label.textContent=centerLabel;
      layer.appendChild(label)
    }
    if(srcLabel){
      const label=document.createElementNS("http://www.w3.org/2000/svg","text");
      label.setAttribute("x",fromX+4);
      label.setAttribute("y",fromY-4);
      label.setAttribute("text-anchor","start");
      label.setAttribute("font-size","9");
      label.setAttribute("fill","#9ca3af");
      label.textContent=srcLabel;
      layer.appendChild(label)
    }
    if(tgtLabel){
      const label=document.createElementNS("http://www.w3.org/2000/svg","text");
      label.setAttribute("x",toX-4);
      label.setAttribute("y",toY-4);
      label.setAttribute("text-anchor","end");
      label.setAttribute("font-size","9");
      label.setAttribute("fill","#9ca3af");
      label.textContent=tgtLabel;
      layer.appendChild(label)
    }
  })
}
function updateSelectionStatus(){
  if(State.selectedRelationId){
    Canvas.statusSelection.textContent="Relation selected";
    return
  }
  const tids=Array.from(State.selectedTableIds);
  if(tids.length===0){Canvas.statusSelection.textContent="No selection";return}
  const t=getTableById(tids[0]);
  if(!t){Canvas.statusSelection.textContent="No selection";return}
  const prefix=t.schemaId?(getSchemaById(t.schemaId)?.name+"."):"";
  if(State.selectedFieldId){
    const f=t.fields.find(x=>x.id===State.selectedFieldId);
    if(f)Canvas.statusSelection.textContent="Field: "+prefix+t.name+"."+f.name;
    else Canvas.statusSelection.textContent="Table: "+prefix+t.name
  }else Canvas.statusSelection.textContent="Table: "+prefix+t.name
}
RightPanel.newTableBtn.addEventListener("click",()=>{
  ensureSchemas();
  const s=State.schemas[0];
  const t=createTable("table_"+(State.tables.length+1),s.id,120+State.tables.length*40,120+State.tables.length*30);
  State.tables.push(t);
  State.selectedTableIds=new Set([t.id]);
  State.selectedFieldId=null;
  State.selectedRelationId=null;
  updateAllUI()
});
RightPanel.tableSelect.addEventListener("change",()=>{
  const id=RightPanel.tableSelect.value;
  if(id){State.selectedTableIds=new Set([id]);State.selectedFieldId=null;State.selectedRelationId=null;updateAllUI()}
});
RightPanel.deleteBtn.addEventListener("click",()=>handleDelete());
function handleDelete(){
  if(State.selectedRelationId){
    State.relations=State.relations.filter(r=>r.id!==State.selectedRelationId);
    State.selectedRelationId=null;
    updateAllUI();
    return
  }
  if(State.selectedFieldId){
    const tids=Array.from(State.selectedTableIds);
    if(!tids.length)return;
    const t=getTableById(tids[0]);
    if(!t)return;
    t.fields=t.fields.filter(f=>f.id!==State.selectedFieldId);
    State.relations=State.relations.filter(r=>r.fromFieldId!==State.selectedFieldId&&r.toFieldId!==State.selectedFieldId);
    State.selectedFieldId=null;
    updateAllUI();
    return
  }
  const tids=Array.from(State.selectedTableIds);
  if(!tids.length)return;
  const id=tids[0];
  State.tables=State.tables.filter(t=>t.id!==id);
  State.relations=State.relations.filter(r=>r.fromTableId!==id&&r.toTableId!==id);
  State.selectedTableIds=new Set();
  State.selectedFieldId=null;
  State.selectedRelationId=null;
  if(State.tables.length)State.selectedTableIds.add(State.tables[0].id);
  updateAllUI()
}
document.addEventListener("keydown",e=>{
  if((e.key==="Delete"||e.key==="Backspace")&&!["INPUT","TEXTAREA","SELECT"].includes(e.target.tagName)){e.preventDefault();handleDelete()}
  if(e.ctrlKey||e.metaKey){
    if(e.key==="+"||e.key==="="){e.preventDefault();setZoom(State.zoom+0.1)}
    if(e.key==="-" ){e.preventDefault();setZoom(State.zoom-0.1)}
  }
});
RightPanel.addFieldBtn.addEventListener("click",()=>{
  const tids=Array.from(State.selectedTableIds);
  if(!tids.length)return;
  const t=getTableById(tids[0]);
  if(!t)return;
  const f=createField();
  t.fields.push(f);
  State.selectedFieldId=f.id;
  updateAllUI()
});

function moveSelectedField(direction){
  const tids=Array.from(State.selectedTableIds);
  const table=tids.length?getTableById(tids[0]):null;
  if(!table||!State.selectedFieldId)return;
  const idx=table.fields.findIndex(f=>f.id===State.selectedFieldId);
  if(idx===-1)return;
  const target=idx+direction;
  if(target<0||target>=table.fields.length)return;
  const moving=table.fields[idx];
  table.fields.splice(idx,1);
  table.fields.splice(target,0,moving);
  State.selectedFieldId=moving.id;
  updateAllUI()
}
RightPanel.moveFieldUp.addEventListener("click",()=>moveSelectedField(-1));
RightPanel.moveFieldDown.addEventListener("click",()=>moveSelectedField(1));
["fldName","fldFaName","fldType","fldNullable","fldPrimary","fldDescription","fldPinned"].forEach(id=>{
  const el=RightPanel[id];
  const handler=()=>saveFieldEditor();
  if(el.type==="checkbox")el.addEventListener("change",handler);else el.addEventListener("input",handler)
});
RightPanel.tblName.addEventListener("input",()=>saveTableEditor());
RightPanel.tblFaName.addEventListener("input",()=>saveTableEditor());
RightPanel.tblDescription.addEventListener("input",()=>saveTableEditor());
RightPanel.tblDeleteBehavior.addEventListener("change",()=>saveTableEditor());
RightPanel.tblSchemaSelect.addEventListener("change",()=>saveTableEditor());
function updateTableSelect(){
  const sel=RightPanel.tableSelect;
  sel.innerHTML="";
  State.tables.forEach(t=>{
    const opt=document.createElement("option");
    opt.value=t.id;
    opt.textContent=(getSchemaById(t.schemaId)?.name||"")+"."+t.name;
    sel.appendChild(opt)
  });
  const tids=Array.from(State.selectedTableIds);
  if(tids.length&&getTableById(tids[0]))sel.value=tids[0]
}
function updateTableEditor(){
  const tids=Array.from(State.selectedTableIds);
  const t=tids.length?getTableById(tids[0]):null;
  if(!t){
    RightPanel.tblName.value="";
    RightPanel.tblFaName.value="";
    RightPanel.tblDescription.value="";
    RightPanel.tblDeleteBehavior.value="none";
    RightPanel.tblSchemaSelect.value="";
    return
  }
  RightPanel.tblName.value=t.name;
  RightPanel.tblFaName.value=t.faName||"";
  RightPanel.tblDescription.value=t.description||"";
  RightPanel.tblDeleteBehavior.value=t.deleteBehavior||"none";
  RightPanel.tblSchemaSelect.value=t.schemaId||""
}
function saveTableEditor(){
  const tids=Array.from(State.selectedTableIds);
  const t=tids.length?getTableById(tids[0]):null;
  if(!t)return;
  t.name=RightPanel.tblName.value.trim()||"table";
  t.faName=RightPanel.tblFaName.value.trim();
  t.description=RightPanel.tblDescription.value.trim();
  t.deleteBehavior=RightPanel.tblDeleteBehavior.value||"none";
  t.schemaId=RightPanel.tblSchemaSelect.value||null;
  updateAllUI()
}
function updateFieldEditor(){
  const tids=Array.from(State.selectedTableIds);
  const t=tids.length?getTableById(tids[0]):null;
  const f=t?getFieldById(t.id,State.selectedFieldId):null;
  if(!f){
    RightPanel.fldName.value="";
    RightPanel.fldFaName.value="";
    RightPanel.fldDescription.value="";
    RightPanel.fldNullable.checked=true;
    RightPanel.fldPrimary.checked=false;
    RightPanel.fldPinned.checked=false;
    if(RightPanel.fldTypeParams)RightPanel.fldTypeParams.value="";
    return
  }
  RightPanel.fldName.value=f.name;
  RightPanel.fldFaName.value=f.faName||"";
  RightPanel.fldDescription.value=f.description||"";
  RightPanel.fldNullable.checked=f.nullable;
  RightPanel.fldPrimary.checked=f.primary;
  RightPanel.fldPinned.checked=!!f.pinned;
  updateFieldTypeOptions();
  const parsed=parseTypeWithParams(f.type);
  RightPanel.fldType.value=parsed.base;
  RightPanel.fldTypeParams.value=parsed.params;
  toggleTypeParamsVisibility()
}
function updateFieldTypeOptions(){
  const sel=RightPanel.fldType;
  const types=DataTypes[State.dbType]||["TEXT"];
  const current=sel.value;
  sel.innerHTML="";
  types.forEach(t=>{
    const op=document.createElement("option");
    op.value=t;
    op.textContent=t;
    sel.appendChild(op)
  });
  if(current&&!types.includes(current)){
    const op=document.createElement("option");
    op.value=current;
    op.textContent=current;
    sel.appendChild(op)
  }
  toggleTypeParamsVisibility()
}
function parseTypeWithParams(value){
  const m=(value||"").match(/^([^(]+)\(([^)]+)\)$/);
  if(m)return{base:m[1],params:m[2]};
  return{base:value||"",params:""};
}
function combineTypeAndParams(base,params){
  if(!base)return"";
  if(params)return base+"("+params+")";
  return base;
}
function toggleTypeParamsVisibility(){
  if(!RightPanel.fldType||!RightPanel.fldTypeParams)return;
  const base=RightPanel.fldType.value.toLowerCase();
  const needsParam=/(char|varchar|nchar|nvarchar|binary|varbinary|decimal|numeric)/.test(base);
  RightPanel.fldTypeParams.parentElement.style.display=needsParam?"block":"none";
}
function saveFieldEditor(){
  const tids=Array.from(State.selectedTableIds);
  const t=tids.length?getTableById(tids[0]):null;
  if(!t)return;
  const f=getFieldById(t.id,State.selectedFieldId);
  if(!f)return;
  f.name=RightPanel.fldName.value.trim()||"field";
  f.faName=RightPanel.fldFaName.value.trim();
  f.description=RightPanel.fldDescription.value.trim();
  f.type=combineTypeAndParams(RightPanel.fldType.value,RightPanel.fldTypeParams.value.trim());
  f.nullable=RightPanel.fldNullable.checked;
  f.primary=RightPanel.fldPrimary.checked;
  f.pinned=RightPanel.fldPinned.checked;
  updateAllUI()
}
function syncSchemaSelects(){
  ensureSchemas();
  const sels=[RightPanel.tblSchemaSelect,RightPanel.relFromTable,RightPanel.relToTable];
  sels.forEach(sel=>{
    if(!sel)return;
    const cur=sel.value;
    sel.innerHTML="";
    if(sel===RightPanel.tblSchemaSelect){
      State.schemas.forEach(s=>{
        const op=document.createElement("option");
        op.value=s.id;
        op.textContent=s.name;
        sel.appendChild(op)
      })
    }
  });
  updateRelationsTableSelects();
  updateSchemaManager()
}
function updateRelationsTableSelects(){
  const sels=[RightPanel.relFromTable,RightPanel.relToTable];
  sels.forEach(sel=>{
    const cur=sel.value;
    sel.innerHTML="";
    const op0=document.createElement("option");
    op0.value="";
    op0.textContent="Select table";
    sel.appendChild(op0);
    State.tables.forEach(t=>{
      const op=document.createElement("option");
      op.value=t.id;
      op.textContent=(getSchemaById(t.schemaId)?.name||"")+". "+t.name;
      sel.appendChild(op)
    });
    sel.value=cur
  });
  updateRelationFieldSelects()
}
function updateRelationFieldSelects(){
  const ft=getTableById(RightPanel.relFromTable.value);
  const tt=getTableById(RightPanel.relToTable.value);
  RightPanel.relFromField.innerHTML="";
  RightPanel.relToField.innerHTML="";
  if(ft){
    ft.fields.forEach(f=>{
      const op=document.createElement("option");
      op.value=f.id;
      op.textContent=f.name;
      RightPanel.relFromField.appendChild(op)
    })
  }
  if(tt){
    tt.fields.forEach(f=>{
      const op=document.createElement("option");
      op.value=f.id;
      op.textContent=f.name;
      RightPanel.relToField.appendChild(op)
    })
  }
}
RightPanel.relFromTable.addEventListener("change",updateRelationFieldSelects);
RightPanel.relToTable.addEventListener("change",updateRelationFieldSelects);
RightPanel.addRelationBtn.addEventListener("click",()=>{
  const ftId=RightPanel.relFromTable.value;
  const ffId=RightPanel.relFromField.value;
  const ttId=RightPanel.relToTable.value;
  const tfId=RightPanel.relToField.value;
  const onDelete=RightPanel.relOnDelete.value;
  if(!ftId||!ffId||!ttId||!tfId)return;
  const rel=createRelation(ftId,ffId,ttId,tfId,onDelete);
  State.relations.push(rel);
  State.selectedRelationId=rel.id;
  State.selectedTableIds=new Set();
  State.selectedFieldId=null;
  updateAllUI()
});
["relSourceLabel","relCenterLabel","relTargetLabel","relStyle"].forEach(id=>{
  RightPanel[id].addEventListener("input",()=>saveSelectedRelation())
});
RightPanel.relColorA.addEventListener("input",()=>saveSelectedRelation());
RightPanel.relColorB.addEventListener("input",()=>saveSelectedRelation());
RightPanel.relBend.addEventListener("input",()=>saveSelectedRelation());
RightPanel.fldType.addEventListener("change",()=>toggleTypeParamsVisibility());
setMode("move");
function updateRelationsEditor(){
  const rel=State.relations.find(r=>r.id===State.selectedRelationId)||null;
  if(rel){
    RightPanel.relSourceLabel.value=rel.sourceLabel||"";
    RightPanel.relCenterLabel.value=rel.centerLabel||"";
    RightPanel.relTargetLabel.value=rel.targetLabel||"";
    RightPanel.relStyle.value=rel.style||"schema";
    RightPanel.relColorA.value=rel.colorA||"#38bdf8";
    RightPanel.relColorB.value=rel.colorB||"#8b5cf6";
    RightPanel.relBend.value=typeof rel.bendOffset==="number"?rel.bendOffset:0;
  }else{
    RightPanel.relSourceLabel.value="";
    RightPanel.relCenterLabel.value="";
    RightPanel.relTargetLabel.value="";
    RightPanel.relStyle.value="schema";
    RightPanel.relBend.value=0;
  }
  renderRelationsList()
}
function saveSelectedRelation(){
  const rel=State.relations.find(r=>r.id===State.selectedRelationId)||null;
  if(!rel)return;
  rel.sourceLabel=RightPanel.relSourceLabel.value.trim();
  rel.centerLabel=RightPanel.relCenterLabel.value.trim();
  rel.targetLabel=RightPanel.relTargetLabel.value.trim();
  rel.style=RightPanel.relStyle.value;
  rel.colorA=RightPanel.relColorA.value;
  rel.colorB=RightPanel.relColorB.value;
  rel.bendOffset=Number(RightPanel.relBend.value)||0;
  updateAllUI()
}
function renderRelationsList(){
  const list=RightPanel.relationsList;
  list.innerHTML="";
  if(!State.relations.length){
    const div=document.createElement("div");
    div.className="text-[10px] text-slate-500";
    div.textContent="No relations defined.";
    list.appendChild(div);
    return
  }
  State.relations.forEach(rel=>{
    const ft=getTableById(rel.fromTableId);
    const tt=getTableById(rel.toTableId);
    const ff=getFieldById(rel.fromTableId,rel.fromFieldId);
    const tf=getFieldById(rel.toTableId,rel.toFieldId);
    if(!ft||!tt||!ff||!tf)return;
    const row=document.createElement("div");
    row.className="flex items-center justify-between gap-2 px-2 py-1 rounded-md border "+(State.selectedRelationId===rel.id?"border-orange-500 bg-slate-900":"border-slate-800 bg-slate-950");
    const left=document.createElement("div");
    const l1=document.createElement("div");
    l1.className="text-[11px]";
    l1.textContent=ft.name+"."+ff.name+" -> "+tt.name+"."+tf.name;
    const l2=document.createElement("div");
    l2.className="text-[10px] text-slate-400";
    l2.textContent="ON DELETE "+rel.onDelete.toUpperCase().replace("_"," ");
    left.appendChild(l1);left.appendChild(l2);
    const right=document.createElement("div");
    right.className="flex items-center gap-1";
    const selBtn=document.createElement("button");
    selBtn.className="px-2 py-0.5 rounded-md border border-slate-700 text-[10px] hover:bg-slate-900";
    selBtn.textContent="Select";
    selBtn.addEventListener("click",e=>{
      e.stopPropagation();
      State.selectedRelationId=rel.id;
      State.selectedTableIds=new Set();
      State.selectedFieldId=null;
      updateAllUI()
    });
    const delBtn=document.createElement("button");
    delBtn.className="px-2 py-0.5 rounded-md border border-red-600 text-red-300 text-[10px] hover:bg-red-950";
    delBtn.textContent="Delete";
    delBtn.addEventListener("click",e=>{
      e.stopPropagation();
      State.relations=State.relations.filter(r=>r.id!==rel.id);
      if(State.selectedRelationId===rel.id)State.selectedRelationId=null;
      updateAllUI()
    });
    right.appendChild(selBtn);right.appendChild(delBtn);
    row.appendChild(left);row.appendChild(right);
  list.appendChild(row)
 })
}
RightPanel.addSchemaBtn.addEventListener("click",()=>{
  const s={id:uuid(),name:"schema_"+(State.schemas.length+1),faName:"",colorA:"#38bdf8",colorB:"#8b5cf6"};
  State.schemas.push(s);
  updateSchemaManager();
  RightPanel.schemaList.value=s.id;
  loadSchemaIntoEditor()
});
RightPanel.schemaList.addEventListener("change",()=>loadSchemaIntoEditor());
RightPanel.saveSchemaBtn.addEventListener("click",()=>{
  const s=State.schemas.find(x=>x.id===RightPanel.schemaList.value);
  if(!s)return;
  s.name=RightPanel.schemaName.value.trim()||"schema";
  s.faName=RightPanel.schemaFaName.value.trim();
  s.colorA=RightPanel.schemaColorA.value||"#38bdf8";
  s.colorB=RightPanel.schemaColorB.value||"#8b5cf6";
  updateAllUI()
});
RightPanel.deleteSchemaBtn.addEventListener("click",()=>{
  const id=RightPanel.schemaList.value;
  if(!id)return;
  State.schemas=State.schemas.filter(s=>s.id!==id);
  State.tables.forEach(t=>{if(t.schemaId===id)t.schemaId=null});
  ensureSchemas();
  updateAllUI()
});
function updateSchemaManager(){
  ensureSchemas();
  const list=RightPanel.schemaList;
  const cur=list.value;
  list.innerHTML="";
  State.schemas.forEach(s=>{
    const op=document.createElement("option");
    op.value=s.id;
    op.textContent=s.name;
    list.appendChild(op)
  });
  list.value=cur||State.schemas[0].id;
  loadSchemaIntoEditor();
  const sel=RightPanel.tblSchemaSelect;
  const prev=sel.value;
  sel.innerHTML="";
  State.schemas.forEach(s=>{
    const op=document.createElement("option");
    op.value=s.id;
    op.textContent=s.name;
    sel.appendChild(op)
  });
  sel.value=prev||State.schemas[0].id
}
function loadSchemaIntoEditor(){
  const s=State.schemas.find(x=>x.id===RightPanel.schemaList.value)||State.schemas[0];
  if(!s)return;
  RightPanel.schemaName.value=s.name;
  RightPanel.schemaFaName.value=s.faName||"";
  RightPanel.schemaColorA.value=s.colorA||"#38bdf8";
  RightPanel.schemaColorB.value=s.colorB||"#8b5cf6"
}
function buildSchemaJson(){
  return{
    dbType:State.dbType,
    dbVersion:State.dbVersion,
    dbName:State.dbName,
    schemas:State.schemas.map(s=>({id:s.id,name:s.name,faName:s.faName,colorA:s.colorA,colorB:s.colorB})),
    tables:State.tables.map(t=>({
      id:t.id,
      name:t.name,
      faName:t.faName,
      description:t.description,
      schemaId:t.schemaId,
      deleteBehavior:t.deleteBehavior,
      x:t.x,
      y:t.y,
      fields:t.fields.map(f=>({
        id:f.id,
        name:f.name,
      faName:f.faName,
      type:f.type,
      nullable:f.nullable,
      primary:f.primary,
      description:f.description,
      pinned:!!f.pinned
    }))
  })),
    relations:State.relations.map(r=>({
      id:r.id,
      fromTableId:r.fromTableId,
      fromFieldId:r.fromFieldId,
      toTableId:r.toTableId,
      toFieldId:r.toFieldId,
      onDelete:r.onDelete,
      style:r.style,
      colorA:r.colorA,
      colorB:r.colorB,
      sourceLabel:r.sourceLabel,
      centerLabel:r.centerLabel,
      targetLabel:r.targetLabel,
      bendOffset:r.bendOffset||0,
      ctrlX:typeof r.ctrlX==="number"?r.ctrlX:null,
      ctrlY:typeof r.ctrlY==="number"?r.ctrlY:null
    }))
  }
}
function updateJsonArea(){
  LeftPanel.jsonArea.value=JSON.stringify(buildSchemaJson(),null,2)
}
LeftPanel.exportJsonBtn.addEventListener("click",updateJsonArea);
LeftPanel.importJsonBtn.addEventListener("click",()=>{
  const txt=LeftPanel.jsonArea.value.trim();
  if(!txt)return;
  try{
    importJsonSchema(txt);
    setDbMeta();
    updateAllUI()
  }catch(e){}
});
LeftPanel.copyJsonBtn.addEventListener("click",()=>{
  LeftPanel.jsonArea.select();
  document.execCommand("copy")
});
LeftPanel.exportSqlBtn.addEventListener("click",updateSqlArea);
LeftPanel.copySqlBtn.addEventListener("click",()=>{
  LeftPanel.sqlArea.select();
  document.execCommand("copy")
});
LeftPanel.applySqlBtn.addEventListener("click",()=>{
  const txt=LeftPanel.sqlArea.value.trim();
  if(!txt)return;
  importSqlText(txt);
  updateAllUI()
});
function importJsonSchema(text){
  const data=JSON.parse(text);
  State.dbType=data.dbType||State.dbType;
  State.dbVersion=data.dbVersion||State.dbVersion;
  State.dbName=data.dbName||State.dbName;
  State.schemas=(data.schemas||[]).map(s=>({id:s.id||uuid(),name:s.name||"schema",faName:s.faName||"",colorA:s.colorA||"#38bdf8",colorB:s.colorB||"#8b5cf6"}));
  if(!State.schemas.length)State.schemas.push(defaultSchema());
  State.tables=(data.tables||[]).map(t=>({
    id:t.id||uuid(),
    name:t.name||"table",
    faName:t.faName||"",
    description:t.description||"",
    schemaId:t.schemaId||State.schemas[0].id,
    deleteBehavior:t.deleteBehavior||"none",
    x:typeof t.x==="number"?t.x:100,
    y:typeof t.y==="number"?t.y:100,
    fields:(t.fields||[]).map(f=>({
      id:f.id||uuid(),
      name:f.name||"field",
      faName:f.faName||"",
      type:f.type||(DataTypes[State.dbType]||["TEXT"])[0],
      nullable:typeof f.nullable==="boolean"?f.nullable:true,
      primary:!!f.primary,
      description:f.description||"",
      pinned:!!f.pinned
    }))
  }));
  State.relations=(data.relations||[]).map(r=>({
    id:r.id||uuid(),
    fromTableId:r.fromTableId,
    fromFieldId:r.fromFieldId,
    toTableId:r.toTableId,
    toFieldId:r.toFieldId,
    onDelete:r.onDelete||"no_action",
    style:r.style||"schema",
    colorA:r.colorA||"#38bdf8",
    colorB:r.colorB||"#8b5cf6",
    sourceLabel:r.sourceLabel||"",
    centerLabel:r.centerLabel||"",
    targetLabel:r.targetLabel||"",
    bendOffset:typeof r.bendOffset==="number"?r.bendOffset:0,
    ctrlX:typeof r.ctrlX==="number"?r.ctrlX:null,
    ctrlY:typeof r.ctrlY==="number"?r.ctrlY:null
  }));
  if(!State.tables.length){
    const t=createTable("table_1",State.schemas[0].id,120,120);
    State.tables.push(t)
  }
  State.selectedTableIds=new Set([State.tables[0].id]);
  State.selectedFieldId=null;
  State.selectedRelationId=null
}
function generateSql(){
  const t=State.dbType;
  const db=State.dbName||"my_database";
  let sql="";
  if(t==="mysql"){sql+="CREATE DATABASE IF NOT EXISTS `"+db+"`;\nUSE `"+db+"`;\n\n"}
  else if(t==="postgres"){sql+="CREATE DATABASE \""+db+"\";\n\\c \""+db+"\";\n\n"}
  else if(t==="sqlserver"){sql+="IF DB_ID('"+db+"') IS NULL CREATE DATABASE ["+db+"]; \nGO\nUSE ["+db+"]; \nGO\n\n"}
  else if(t==="sqlite"){sql+="-- SQLite database file; tables only\n\n"}
  State.tables.forEach((table,idx)=>{
    const schema=getSchemaById(table.schemaId);
    const schemaPrefix=schema?identifier(schema.name,t)+".":"";
    sql+="CREATE TABLE "+schemaPrefix+identifier(table.name,t)+" (\n";
    const lines=[];
    table.fields.forEach(f=>{
      let line="  "+identifier(f.name,t)+" "+f.type;
      line+=f.nullable?" NULL":" NOT NULL";
      lines.push(line)
    });
    const pkFields=table.fields.filter(f=>f.primary);
    if(pkFields.length){
      lines.push("  PRIMARY KEY ("+pkFields.map(f=>identifier(f.name,t)).join(", ")+")")
    }
    const tableRels=State.relations.filter(r=>r.fromTableId===table.id);
    tableRels.forEach(rel=>{
      const toTable=getTableById(rel.toTableId);
      const fromField=getFieldById(rel.fromTableId,rel.fromFieldId);
      const toField=getFieldById(rel.toTableId,rel.toFieldId);
      if(!toTable||!fromField||!toField)return;
      const toSchema=getSchemaById(toTable.schemaId);
      const refPrefix=toSchema?identifier(toSchema.name,t)+".":"";
      let fk="  FOREIGN KEY ("+identifier(fromField.name,t)+") REFERENCES "+refPrefix+identifier(toTable.name,t)+"("+identifier(toField.name,t)+")";
      fk+=sqlOnDeleteClause(rel.onDelete);
      lines.push(fk)
    });
    sql+=lines.join(",\n")+"\n);\n";
    if(table.deleteBehavior&&table.deleteBehavior!=="none")sql+="-- delete behavior: "+table.deleteBehavior+" on dependents\n";
    if(idx<State.tables.length-1)sql+="\n"
  });
  return sql
}
function sqlOnDeleteClause(onDelete){
  if(onDelete==="cascade")return" ON DELETE CASCADE";
  if(onDelete==="set_null")return" ON DELETE SET NULL";
  return""
}
function identifier(name,t){
  if(t==="mysql")return"`"+name+"`";
  if(t==="postgres")return"\""+name+"\"";
  if(t==="sqlserver")return"["+name+"]";
  return"\""+name+"\""
}
function updateSqlArea(){LeftPanel.sqlArea.value=generateSql()}
function importSqlText(text){
  const blocks=text.split(/;/).map(b=>b.trim()).filter(b=>/^CREATE\s+TABLE/i.test(b));
  blocks.forEach((block,idx)=>{
    const parsed=parseCreateTableBlock(block+";");
    if(!parsed)return;
    ensureSchemas();
    let schemaObj=State.schemas.find(s=>s.name===parsed.schema)||null;
    if(parsed.schema&&!schemaObj){
      schemaObj={id:uuid(),name:parsed.schema,faName:"",colorA:"#38bdf8",colorB:"#8b5cf6"};
      State.schemas.push(schemaObj)
    }
    const existing=State.tables.find(t=>t.name===parsed.tableName&&(getSchemaById(t.schemaId)?.name||"")===parsed.schema);
    const baseX=120+idx*70;
    const baseY=120+idx*40;
    if(existing)existing.fields=parsed.fields;
    else{
      const t=createTable(parsed.tableName,schemaObj?schemaObj.id:State.schemas[0].id,baseX,baseY);
      t.fields=parsed.fields;
      State.tables.push(t)
    }
  });
  if(State.tables.length&&!State.selectedTableIds.size)State.selectedTableIds=new Set([State.tables[0].id])
}
function parseCreateTableBlock(block){
  const m=block.match(/CREATE\\s+TABLE\\s+([^(\\s]+)\\s*\\(([^]*)\\)/i);
  if(!m)return null;
  const rawName=m[1].replace(/[\"`\\[\\]]/g,"");
  let schema="";
  let tableName=rawName;
  const dot=rawName.indexOf(".");
  if(dot!==-1){schema=rawName.slice(0,dot);tableName=rawName.slice(dot+1)}
  let body=m[2].trim().replace(/\\r/g,"");
  const lines=body.split("\\n").map(l=>l.trim()).filter(l=>l.length);
  const fields=[];
  lines.forEach(line=>{
    if(line.endsWith(","))line=line.slice(0,-1);
    const upper=line.toUpperCase();
    if(upper.startsWith("PRIMARY KEY")||upper.startsWith("FOREIGN KEY")||upper.startsWith("CONSTRAINT"))return;
    const parts=line.split(/\\s+/);
    if(parts.length<2)return;
    const name=parts[0].replace(/[\"`\\[\\]]/g,"");
    const type=parts[1];
    let nullable=true;
    if(upper.includes("NOT NULL"))nullable=false;
    fields.push({id:uuid(),name,faName:"",type,nullable,primary:false,description:""})
  });
  if(!fields.length)return null;
  return{schema,tableName,fields}
}
LeftPanel.jsonArea.value=JSON.stringify({dbType:State.dbType,dbVersion:State.dbVersion,dbName:State.dbName,tables:[],relations:[]},null,2);
applyTransform();
showView("login");
window.addEventListener("error",e=>{
  console.error("Runtime error",e.message,e.error);
    if(Login.error)Login.error.textContent="خطای غیرمنتظره: "+(err.message||err);
});

