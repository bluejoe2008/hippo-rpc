package org.grapheco.commons.util

import java.lang.reflect.{Field, Modifier}



class ReflectedObject(o: AnyRef) {
  //employee._get("company.name")
  def _get(name: String): AnyRef = {
    try {
      var o2 = o;
      for (fn <- name.split("\\.")) {
        val field = _getField(o2.getClass, fn);
        field.setAccessible(true);
        o2 = field.get(o2);
      }
      o2;
    }
    catch {
      case e: NoSuchFieldException =>
        throw new InvalidFieldPathException(o, name, e);
    }
  }

  def _set(name: String, value: AnyRef): Unit = {
    try {
      var field: Field = null
      var o2: Any = o;
      var o1: Any = null
      for (fn <- name.split("\\.")) {
        field = _getField(o2.getClass, fn);
        field.setAccessible(true);
        o1 = o2;
        o2 = field.get(o2);
      }

      val modifiersField = classOf[Field].getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      field.set(o1, value);
    }
    catch {
      case e: NoSuchFieldException =>
        throw new InvalidFieldPathException(o, name, e);
    }
  }

  private def _getField(clazz: Class[_], fieldName: String): Field = {
    try {
      clazz.getDeclaredField(fieldName);
    }
    catch {
      case e: NoSuchFieldException =>
        val sc = clazz.getSuperclass;
        if (sc == null) {
          throw e;
        }

        _getField(sc, fieldName);
    }
  }

  def _getLazy(name: String): AnyRef = {
    _call(s"$name$$lzycompute")();
  }

  def _call(name: String)(args: Any*): AnyRef = {
    //val method = o.getClass.getDeclaredMethod(name, args.map(_.getClass): _*);
    //TODO: supports overloaded methods?
    val methods = o.getClass.getDeclaredMethods.filter(_.getName.equals(name));
    val method = methods(0);
    method.setAccessible(true);
    method.invoke(o, args.map(_.asInstanceOf[Object]): _*);
  }
}

class InvalidFieldPathException(o: AnyRef, path: String, cause: Throwable)
  extends RuntimeException(s"invalid field path: $path, host: $o", cause) {

}