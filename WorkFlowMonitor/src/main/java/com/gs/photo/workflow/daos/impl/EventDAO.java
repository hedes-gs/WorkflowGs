package com.gs.photo.workflow.daos.impl;

/*
 * public class EventDAO extends JdbcDaoSupport implements IEventDAO {
 * 
 * private static final Logger LOGGER = LoggerFactory.getLogger(EventDAO.class);
 * private static final String PARAM_PATH = "PARAM_PATH"; private static final
 * String NODE_ID = "NODE_ID"; private static final String ROOT_ID = "ROOT_ID";
 * private static final String STEP = "STEP"; private static final String
 * DATA_EVENT = "DATA_EVENT"; private static final String NEW_STATE =
 * "NEW_STATE"; private static final String INITIAL_STATE = "INITIAL_STATE";
 * private static final Gson gson = new GsonBuilder().create(); private static
 * final String SQL_COUNT = "select count(1) from wf_postgres.event "; private
 * static final String SQL_TRUNCATE = "truncate wf_postgres.event ";
 * 
 * @Autowired protected DataSource dataSource;
 * 
 * @PostConstruct protected void init() { super.setDataSource(this.dataSource);
 * }
 * 
 * @Override
 * 
 * @Transactional public void addOrCreate(WfEvents events) {
 * 
 * StoredProcedure procedure = new GenericStoredProcedure();
 * procedure.setDataSource(this.dataSource);
 * procedure.setSql("wf_postgres.create_event_node");
 * procedure.setFunction(true);
 * 
 * SqlParameter[] parameters = { new SqlParameter(EventDAO.PARAM_PATH,
 * Types.OTHER), new SqlParameter(EventDAO.NODE_ID, Types.VARCHAR), new
 * SqlParameter(EventDAO.ROOT_ID, Types.VARCHAR), new
 * SqlParameter(EventDAO.STEP, Types.OTHER), new
 * SqlParameter(EventDAO.DATA_EVENT, Types.OTHER), new
 * SqlParameter(EventDAO.NEW_STATE, Types.OTHER), new
 * SqlParameter(EventDAO.INITIAL_STATE, Types.OTHER) };
 * 
 * procedure.setParameters(parameters); procedure.compile();
 * 
 * events.getEvents() .forEach((evt) -> this.createWfEntity(evt, procedure)); }
 * 
 * @Override public boolean isRootNodeCompleted(String rootId) { return false; }
 * 
 * protected void createWfEntity(final WfEvent evt, StoredProcedure procedure) {
 * String json = EventDAO.gson.toJson(evt);
 * 
 * switch (evt.getStep() .getStep()) { case
 * WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ: case
 * WfEventStep.CREATED_FROM_STEP_IMG_PROCESSOR: case
 * WfEventStep.CREATED_FROM_STEP_PREPARE_FOR_PERSIST: { String rootNode =
 * this.getRootNode(evt); String dataNodeId = this.getDataNodeId(evt); String
 * parentNodeId = this.getParentNodeId(evt); procedure.execute(
 * this.buildEvtPath(rootNode, parentNodeId), dataNodeId, rootNode,
 * evt.getStep() .getStep(), json, "created", "created"); break; } case
 * WfEventStep.CREATED_FROM_STEP_ARCHIVED_IN_HDFS: case
 * WfEventStep.CREATED_FROM_STEP_RECORDED_IN_HBASE: { String rootNode =
 * this.getRootNode(evt); String dataNodeId = this.getDataNodeId(evt); String
 * parentNodeId = this.getParentNodeId(evt); procedure.execute(
 * this.buildEvtPath(rootNode, parentNodeId), dataNodeId, rootNode,
 * evt.getStep() .getStep(), json, "processed", "created"); break; } } }
 * 
 * protected String buildEvtPath(String rootNode, String parentNodeId) {
 * StringJoiner retValue = new StringJoiner("."); retValue.add(rootNode)
 * .add(parentNodeId); return retValue.toString(); }
 * 
 * private String getParentNodeId(WfEvent evt) { return evt.getParentDataId(); }
 * 
 * private String getDataNodeId(WfEvent evt) { return evt.getDataId(); }
 * 
 * private String getRootNode(WfEvent evt) { return evt.getImgId(); }
 * 
 * @Override public int getNbOfEvents() { return this.getJdbcTemplate()
 * .query(EventDAO.SQL_COUNT, (RowMapper<Integer>) (rs, rowNum) -> rs.getInt(1))
 * .get(0); }
 * 
 * @Override public void truncate() { this.getJdbcTemplate()
 * .execute(EventDAO.SQL_TRUNCATE); }
 * 
 * }
 * 
 */