/*
 * package com.gs.photo.workflow.daos.impl;
 *
 * import java.io.Serializable; import java.sql.PreparedStatement; import
 * java.sql.ResultSet; import java.sql.SQLException; import java.sql.Types;
 *
 * import org.hibernate.HibernateException; import
 * org.hibernate.engine.spi.SharedSessionContractImplementor; import
 * org.hibernate.usertype.UserType;
 *
 * public class GenericPostgresType implements UserType {
 *
 * @Override public int[] sqlTypes() { return new int[] { Types.OTHER }; }
 *
 * @SuppressWarnings("rawtypes")
 *
 * @Override public Class returnedClass() { return String.class; }
 *
 * @Override public boolean equals(Object x, Object y) throws HibernateException
 * { return x.equals(y); }
 *
 * @Override public int hashCode(Object x) throws HibernateException { return
 * x.hashCode(); }
 *
 * @Override public Object deepCopy(Object value) throws HibernateException {
 * return new String((String) value); }
 *
 * @Override public boolean isMutable() { return false; }
 *
 * @Override public Serializable disassemble(Object value) throws
 * HibernateException { return (Serializable) value; }
 *
 * @Override public Object assemble(Serializable cached, Object owner) throws
 * HibernateException { return cached; }
 *
 * @Override public Object replace(Object original, Object target, Object owner)
 * throws HibernateException { // TODO Auto-generated method stub return
 * this.deepCopy(original); }
 *
 * @Override public Object nullSafeGet(ResultSet rs, String[] names,
 * SharedSessionContractImplementor session, Object owner) throws
 * HibernateException, SQLException { return rs.getString(names[0]);
 *
 * }
 *
 * @Override public void nullSafeSet(PreparedStatement st, Object value, int
 * index, SharedSessionContractImplementor session) throws HibernateException,
 * SQLException { st.setObject(index, value, Types.OTHER);
 *
 * }
 *
 * }
 */