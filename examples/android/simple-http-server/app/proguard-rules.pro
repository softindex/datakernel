-verbose

-keepattributes Signature # for generic type
-keepattributes InnerClasses # for inner classes
-keepattributes Exceptions # for throw Eceptions
-keepattributes LineNumberTable # save line numbers
-keepattributes *Annotation* # for annotations
-keepattributes EnclosingMethod # for reflection

-keep class io.datakernel.android.** { *; }

-keepclassmembers class * {
    @fully.qualified.package.AnnotationType *;
}