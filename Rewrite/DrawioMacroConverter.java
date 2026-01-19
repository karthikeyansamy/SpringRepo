package my.company.confluence.migration;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.confluence.filter.Macro;
import org.xwiki.contrib.confluence.filter.MacroConverter;
import org.xwiki.filter.FilterException;
import org.xwiki.filter.Listener;
import org.xwiki.filter.event.meta.DocumentMetaData;
import org.xwiki.filter.event.meta.MetaData;
import org.xwiki.filter.event.xwiki.XWikiMacroBlock;
import org.xwiki.filter.event.xwiki.XWikiDocumentBlock;
import org.xwiki.filter.event.xwiki.XWikiRawBlock;
import org.xwiki.filter.util.FilterContext;
import org.xwiki.filter.event.model.Attachment;

@Component(hint = "drawio")
public class DrawioMacroConverter implements MacroConverter
{
    @Override
    public void convert(Macro macro, Listener listener) throws FilterException
    {
        Map<String, String> parameters = macro.getParameters();
        String fileName = parameters.get("diagramName");
        if (fileName == null) {
            return;
        }

        String childPageName = fileName.replace(".drawio", "");

        // ------------------------------------------------------------
        // 1️⃣ Parent page: emit {{diagram reference="childPageName"/}}
        // ------------------------------------------------------------
        listener.onEvent(new XWikiMacroBlock(
            "diagram",
            Collections.singletonMap("reference", childPageName),
            false
        ));

        // ------------------------------------------------------------
        // 2️⃣ Load drawio attachment (mxGraphModel XML)
        // ------------------------------------------------------------
        byte[] xmlBytes = loadAttachment(fileName, listener);
        if (xmlBytes == null) {
            return;
        }

        String mxGraphXml = new String(xmlBytes, StandardCharsets.UTF_8);

        // ------------------------------------------------------------
        // 3️⃣ Create child document
        // ------------------------------------------------------------
        DocumentMetaData docMeta = new DocumentMetaData();
        docMeta.setTitle(childPageName);

        listener.onEvent(new XWikiDocumentBlock(childPageName, docMeta));

        // ------------------------------------------------------------
        // 4️⃣ Insert {{drawio}} macro with raw XML
        // ------------------------------------------------------------
        listener.onEvent(new XWikiMacroBlock("drawio", Collections.emptyMap(), false));
        listener.onEvent(new XWikiRawBlock(mxGraphXml));
        listener.onEvent(XWikiMacroBlock.END);

        // ------------------------------------------------------------
        // 5️⃣ End child document
        // ------------------------------------------------------------
        listener.onEvent(XWikiDocumentBlock.END);
    }

    private byte[] loadAttachment(String fileName, Listener listener)
    {
        FilterContext context = listener.getContext();
        if (context == null) {
            return null;
        }

        for (Attachment attachment : context.getAttachments()) {
            if (fileName.equals(attachment.getName())) {
                return attachment.getContent();
            }
        }
        return null;
    }
}
