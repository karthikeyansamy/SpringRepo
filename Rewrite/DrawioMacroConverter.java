package org.xwiki.contrib.confluence.filter.internal.macros;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.confluence.filter.Macro;
import org.xwiki.contrib.confluence.filter.MacroConverter;
import org.xwiki.filter.FilterException;
import org.xwiki.filter.Listener;
import org.xwiki.filter.util.FilterContext;
import org.xwiki.filter.event.model.Attachment;
import org.xwiki.contrib.confluence.filter.internal.ConfluenceConversionContext;
import org.xwiki.contrib.confluence.filter.internal.input.ConfluenceInputProperties;

@Component
@Named("drawio")
@Singleton
public class DrawioMacroConverter implements MacroConverter
{
    @Inject
    private ConfluenceConversionContext conversionContext;

    @Override
    public void convert(Macro macro, Listener listener) throws FilterException
    {
        Map<String, String> parameters = macro.getParameters();
        String diagramFile = parameters.get("diagramName");
        if (diagramFile == null) {
            return;
        }

        String childPageName = diagramFile.replace(".drawio", "");

        // 1️⃣ Emit diagram reference macro in parent page
        listener.beginMacro("diagram",
            Collections.singletonMap("reference", childPageName),
            false);
        listener.endMacro("diagram");

        // 2️⃣ Load drawio attachment
        byte[] drawioXml = loadAttachment(diagramFile, listener);
        if (drawioXml == null) {
            return;
        }

        String diagramXml = new String(drawioXml, StandardCharsets.UTF_8);

        // 3️⃣ Create child page via conversion context
        this.conversionContext.pushDocument(childPageName);

        listener.beginMacro("drawio", Collections.emptyMap(), false);
        listener.onCharacters(diagramXml);
        listener.endMacro("drawio");

        this.conversionContext.popDocument();
    }

    private byte[] loadAttachment(String name, Listener listener)
    {
        FilterContext context = listener.getContext();
        if (context == null) {
            return null;
        }

        for (Attachment attachment : context.getAttachments()) {
            if (name.equals(attachment.getName())) {
                return attachment.getContent();
            }
        }
        return null;
    }
}
