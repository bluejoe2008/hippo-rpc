package org.grapheco.commons.util

/**
  * Created by bluejoe on 2018/7/5.
  */
object ReflectUtils {
  implicit def reflected(o: AnyRef): ReflectedObject = new ReflectedObject(o);

  def singleton[T](implicit m: Manifest[T]): AnyRef = {
    val field = Class.forName(m.runtimeClass.getName + "$").getDeclaredField("MODULE$");
    field.setAccessible(true);
    field.get();
  }

  def instanceOf[T](args: Any*)(implicit m: Manifest[T]): T = {
    val constructor = m.runtimeClass.getDeclaredConstructor(args.map(_.getClass): _*);
    constructor.setAccessible(true);
    constructor.newInstance(args.map(_.asInstanceOf[Object]): _*).asInstanceOf[T];
  }

  def instanceOf(className: String)(args: Any*): Any = {
    val constructor = Class.forName(className).getDeclaredConstructor(args.map(_.getClass): _*);
    constructor.setAccessible(true);
    constructor.newInstance(args.map(_.asInstanceOf[Object]): _*);
  }
}
