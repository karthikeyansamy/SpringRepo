package org.xwiki.contrib.confluence.filter.internal.macros;

import java.util.Collections;
import java.util.Map;

import javax.inject.Named;
import javax.inject.Singleton;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.confluence.filter.internal.macros.AbstractMacroConverter;
import org.xwiki.filter.FilterContext;
import org.xwiki.filter.event.model.FilterEventParameters;
import org.xwiki.filter.listener.Listener;
import org.xwiki.filter.type.FilterEventType;
import org.xwiki.rendering.macro.Macro;

@Component
@Named("drawio")
@Singleton
public class DrawioMacroConverter extends AbstractMacroConverter
{
    @Override
    public void convert(Macro macro, Listener listener, FilterContext context)
    {
        String diagramName = macro.getParameters().get("diagramName");

        listener.beginMacro(
            "drawio",
            diagramName != null
                ? Collections.singletonMap(
                    "diagram",
                    diagramName + ".drawio.xml"
                  )
                : Collections.emptyMap(),
            false,
            FilterEventParameters.EMPTY
        );

        listener.endMacro("drawio", FilterEventParameters.EMPTY);
    }
}
