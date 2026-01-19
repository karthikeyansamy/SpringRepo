package org.xwiki.contrib.confluence.filter.macros;

import java.util.Map;

import javax.inject.Named;
import javax.inject.Singleton;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.confluence.filter.AbstractMacroConverter;
import org.xwiki.filter.FilterContext;

@Component
@Named("drawio")
@Singleton
public class DrawioMacroConverter extends AbstractMacroConverter
{
    @Override
    protected String toXWikiId(String confluenceId)
    {
        // Confluence macro name → XWiki macro name
        return "drawio";
    }

    @Override
    protected void toXWikiParameters(
        Map<String, String> confluenceParameters,
        Map<String, String> xwikiParameters,
        FilterContext context)
    {
        String diagramName = confluenceParameters.get("diagramName");

        if (diagramName != null && !diagramName.isEmpty()) {
            // Reference already-imported attachment
            xwikiParameters.put("diagram", diagramName + ".drawio.xml");
        }
    }

    @Override
    protected boolean supportsInlineMode()
    {
        // draw.io is a block macro
        return false;
    }
}
