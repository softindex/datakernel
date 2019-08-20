package io.datakernel.docs.module.props;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.dao.FileResourceDao;
import io.datakernel.docs.dao.ResourceDao;
import io.datakernel.docs.plugin.props.NavBarPropertiesPlugin;
import io.datakernel.docs.plugin.props.PropertiesPlugin;

import java.io.FileNotFoundException;

import static io.datakernel.config.ConfigConverters.*;

public final class NavbarPropertiesPluginModule extends AbstractModule {
	@Export
	@ProvidesIntoSet
	PropertiesPlugin<?> navBarsPropertiesPlugin(Config config, ResourceDao resourceDao) {
		return NavBarPropertiesPlugin.create(config.get(ofList(ofString()), "sectors"), resourceDao);
	}

	@Provides
	ResourceDao resourceDao(Config config) throws FileNotFoundException {
		return FileResourceDao.create(config.get(ofPath(), "nav.data"));
	}
}
