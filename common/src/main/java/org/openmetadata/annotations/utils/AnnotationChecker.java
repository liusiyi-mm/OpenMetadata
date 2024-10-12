/**
用于检查某个类及其字段上是否存在指定的注解 
通过反射机制遍历类的字段，查看是否有被某个注解标记的字段
*/
package org.openmetadata.annotations.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import org.openmetadata.annotations.ExposedField;

public class AnnotationChecker {

  /**
  该类为一个工具类，它的构造函数是私有的（private），这意味着它不能被实例化，所有的方法都是静态的，直接通过类名调用。
  这是一个典型的“工具类”设计模式，所有方法都是静态的，不需要实例化类。（直接用工具）
  */
  private AnnotationChecker() {}

  /**
  为什么需要两个方法？
1.简化调用: 第一个方法提供了一个简洁的调用接口，用户不需要每次调用都手动传递一个visitedClasses集合。
该集合的创建和管理交给了框架处理，用户只需要传递最基本的参数（类和注解类）。这样既简化了代码使用，又隐藏了复杂的递归逻辑。
2.扩展性和灵活性: 虽然大多数情况下，开发者可能会调用第一个简化版的方法，但有时也需要提供第二个方法供内部或高级调用使用
  */
  private static boolean checkIfAnyClassFieldsHasAnnotation(
      Class<?> objectClass, Class<? extends Annotation> annotationClass) {
    return checkIfAnyClassFieldsHasAnnotation(objectClass, annotationClass, new HashSet<>());
  }

  private static boolean checkIfAnyClassFieldsHasAnnotation(
      // 参数 visitedClasses，用于跟踪哪些类已经访问过，以防止在处理复杂类结构时，出现循环引用（即类A引用类B，而类B又引用类A，形成无限递归）。
      Class<?> objectClass,  // Class类的对象内容是你创建的类的类型信息
      Class<? extends Annotation> annotationClass,
      Set<Class<?>> visitedClasses) {
    for (Field field : objectClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(annotationClass)) {
        return true;
      }
      if (!field.getType().isPrimitive() && !visitedClasses.contains(field.getType())) {
        visitedClasses.add(field.getType()); 
        // 逻辑：如果该字段不是原始类型，并且还没有在 visitedClasses 集合中（意味着没有被检查过），则将该字段的类型添加到 visitedClasses 中，并递归检查该字段类型的所有字段。
        if (checkIfAnyClassFieldsHasAnnotation(field.getType(), annotationClass, visitedClasses)) {
          return true;
        }
      }
    }
    
    if (objectClass.getSuperclass() != null
        && !visitedClasses.contains(objectClass.getSuperclass())) {
      visitedClasses.add(objectClass.getSuperclass());
      return checkIfAnyClassFieldsHasAnnotation(
          objectClass.getSuperclass(), annotationClass, visitedClasses);
    }
    return false;
  }

  /** Check if any field of a given class has the annotation {@link ExposedField}. */
  public static boolean isExposedFieldPresent(Class<?> objectClass) {
    return checkIfAnyClassFieldsHasAnnotation(objectClass, ExposedField.class);
  }
}
